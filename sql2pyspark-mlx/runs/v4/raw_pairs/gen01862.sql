with filtered as (
    -- Keep recent, valid rows
    select
        *,
        trim(store_name) as clean_store_name,
        coalesce(region, 'N/A') as region_clean
    from stores
    where opened_date >= date '2023-04-30'
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        concat_ws('-', store_name, region) as store_name_region_key,
        coalesce(region, 'N/A') as resolved_region
    from filtered
    where opened_date > date '2023-12-02'
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        avg(sqft) over (partition by city) as city_avg_sqft,
        sum(sqft) over (partition by city order by opened_date, id rows between unbounded preceding and current row) as running_sqft
    from cleaned
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from metrics
    qualify rank() over (partition by region order by city_avg_sqft desc nulls first) = 1
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        cume_dist() over (partition by region order by running_sqft nulls last) as running_sqft_cume_dist,
        sum(running_sqft) over (partition by region order by sqft, id rows between unbounded preceding and current row) as cumulative_running_sqft,
        avg(city_avg_sqft) over (order by city_avg_sqft, id rows between 2 preceding and current row) as city_avg_sqft_avg_2,
        lag(sqft, 2) over (partition by region order by sqft, id) as prev_sqft
    from latest
),
with_latest as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.order_status,
        x.discount
    from scored s
    left join lateral (
            select c.status as order_status, c.discount
            from orders c
            where c.store_id = s.id
            order by c.order_date desc, c.id desc
            limit 1
        ) x on true
),
matched_keys as (
    -- Set intersection on keys
    select id
    from with_latest
    intersect all
    select id
    from stores
    where city = 'Singapore'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_latest s
    inner join matched_keys k on s.id = k.id
),
totals as (
    -- Multi-level aggregation
    select
        case when discount >= 0.36 then 'HIGH' else 'LOW' end as discount_tier,
        count(*) filter (where city = 'Berlin') as berlin_rows,
        percentile_approx(cumulative_running_sqft, array(0.25, 0.5, 0.75)) as cumulative_running_sqft_quartiles,
        count(case when city = 'Tokyo' then 1 end) as tokyo_count
    from kept_rows
    group by cube (case when discount >= 0.36 then 'HIGH' else 'LOW' end)
    having tokyo_count > 1
)
select
    discount_tier,
    cumulative_running_sqft_quartiles,
    tokyo_count
from totals
