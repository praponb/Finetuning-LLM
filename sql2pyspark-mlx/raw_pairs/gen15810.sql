with base as (
    -- Basic cleansing and defaults
    select *, round(sqft, 1) as sqft_rounded
    from stores
    where opened_date >= to_date('2023-04-29')
      and region is not distinct from 'Central'
),
with_last as (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    select s.*, x.sale_date
    from base s
    left join lateral (
            select c.sale_date
            from store_sales c
            where c.store_id = s.id
            order by c.revenue desc, c.id desc
            limit 1
        ) x on true
),
matched_keys as (
    -- Set intersection on keys
    select id, city
    from with_last
    intersect all
    select id, city
    from stores
    where sqft > 15644
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_last s
    inner join matched_keys k on s.id = k.id and s.city = k.city
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        store_name,
        city,
        region,
        sqft,
        rank() over (partition by region order by sqft nulls last) as sqft_rank,
        percent_rank() over (order by sqft_rounded nulls first) as sqft_rounded_percent_rank,
        count(sqft) over (partition by year(sale_date), region) as sale_date_year_count_sqft,
        dense_rank() over (partition by region order by sqft desc nulls last) as sqft_dense_rank
    from verified
    where city in ('Tokyo', 'Singapore')
),
metrics_stores as (
    -- Running and moving aggregates
    select
        id,
        store_name,
        region,
        sqft,
        sqft_rounded_percent_rank,
        dense_rank() over (partition by city order by sqft_rank desc) as sqft_rank_dense_rank,
        cume_dist() over (partition by region, city order by sqft desc nulls first) as sqft_cume_dist,
        cume_dist() over (partition by city order by sqft desc) as sqft_cume_dist_2,
        round(sqft_dense_rank / sum(sqft_dense_rank) over (partition by city), 4) as sqft_dense_rank_share,
        avg(sqft_rounded_percent_rank) over (partition by city) as city_avg_sqft_rounded_percent_rank
    from metrics
),
enriched as (
    -- Running and moving aggregates
    select
        store_name,
        region,
        sqft_rank_dense_rank,
        sqft_cume_dist,
        sqft_cume_dist_2,
        sqft_dense_rank_share,
        city_avg_sqft_rounded_percent_rank,
        sum(sqft_rounded_percent_rank) over (partition by region) as region_sum_sqft_rounded_percent_rank,
        max(sqft_rank_dense_rank) over (partition by region) as region_max_sqft_rank_dense_rank,
        sum(sqft_cume_dist_2) over (partition by region) as region_sum_sqft_cume_dist_2
    from metrics_stores
),
grouped as (
    -- Multi-level aggregation
    select
        case when region_sum_sqft_rounded_percent_rank >= 4 then 'HIGH' else 'LOW' end as region_sum_sqft_rounded_percent_rank_tier,
        sort_array(collect_set(region)) as region_list,
        sort_array(collect_set(region)) as region_list_2,
        sort_array(collect_set(region)) as region_list_3
    from enriched
    group by 1
)
select
    region_sum_sqft_rounded_percent_rank_tier,
    region_list_2,
    region_list_3
from grouped
