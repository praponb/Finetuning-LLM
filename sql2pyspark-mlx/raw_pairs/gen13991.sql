with with_windows as (
    select
        *,
        ntile(5) over w_grp as revenue_bucket,
        count(units) over (partition by product_id, store_id) as product_id_count_units,
        lag(units, 1) over w_grp_2 as prev_units
    from store_sales
    window w_grp as (partition by store_id order by revenue desc, id), w_grp_2 as (partition by product_id order by sale_date, id)
),
enriched as (
    select
        *,
        sum(revenue_bucket) over (partition by year(sale_date) order by prev_units, id rows between unbounded preceding and current row) as cumulative_revenue_bucket,
        avg(revenue_bucket) over (partition by year(sale_date), product_id order by prev_units, id rows between 2 preceding and 1 preceding) as revenue_bucket_avg_2_prior
    from with_windows
),
enriched_store_sales as (
    select
        src.id,
        src.product_id,
        src.sale_date,
        src.revenue_bucket,
        src.product_id_count_units,
        src.cumulative_revenue_bucket,
        src.revenue_bucket_avg_2_prior,
        p.launch_date,
        p.price,
        p.product_name
    from enriched src
    join products p
        on src.product_id = p.id
    where p.product_name like '%Mat%'
),
deduped as (
    select *
    from (
        select *, rank() over (partition by product_id order by sale_date) as dedup_rank
        from enriched_store_sales
    ) x
    where dedup_rank = 1
),
kept_keys as (
    select id, product_id
    from deduped
    except
    select id, product_id
    from store_sales
    where store_id in (4, 6)
),
verified as (
    select s.*
    from deduped s
    join kept_keys k on s.id = k.id and s.product_id = k.product_id
),
stats as (
    select
        case when revenue_bucket_avg_2_prior >= 8 then 'PREMIUM' else 'STANDARD' end as revenue_bucket_avg_2_prior_tier,
        date_trunc('year', sale_date) as year_start,
        case when cumulative_revenue_bucket >= 34 then 'HIGH' else 'LOW' end as cumulative_revenue_bucket_tier,
        count(*) as cnt,
        percentile_approx(revenue_bucket, 0.9) as revenue_bucket_p90,
        count(distinct id) as distinct_ids,
        round(avg(cumulative_revenue_bucket), 2) as avg_cumulative_revenue_bucket,
        count(*) as event_count,
        approx_percentile(product_id_count_units, array(0.25, 0.5, 0.75)) as product_id_count_units_quartiles
    from verified
    group by case when revenue_bucket_avg_2_prior >= 8 then 'PREMIUM' else 'STANDARD' end, date_trunc('year', sale_date), case when cumulative_revenue_bucket >= 34 then 'HIGH' else 'LOW' end
    having count(distinct id) > 1
)
select
    revenue_bucket_avg_2_prior_tier,
    year_start,
    cumulative_revenue_bucket_tier,
    cnt,
    revenue_bucket_p90,
    avg_cumulative_revenue_bucket,
    event_count,
    product_id_count_units_quartiles
from stats
order by revenue_bucket_p90 nulls last, cumulative_revenue_bucket_tier desc
