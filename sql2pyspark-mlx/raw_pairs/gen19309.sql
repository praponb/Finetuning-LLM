with scored as (
    -- Running and moving aggregates
    select
        id,
        store_name,
        region,
        opened_date,
        min(sqft) over (partition by region, city order by opened_date asc, id asc rows between unbounded preceding and current row) as running_min_sqft,
        sum(sqft) over (partition by region, city order by opened_date asc, id asc rows between unbounded preceding and current row) as running_sum_sqft,
        count(sqft) over (partition by region, city order by opened_date asc, id asc rows between unbounded preceding and current row) as running_count_sqft,
        max(sqft) over (partition by region, city order by opened_date asc, id asc rows between unbounded preceding and current row) as running_max_sqft
    from stores
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        store_name,
        region,
        opened_date,
        running_sum_sqft,
        running_max_sqft,
        sum(running_sum_sqft) over (partition by region order by running_min_sqft asc, id asc rows between unbounded preceding and current row) as running_sum_running_sum_sqft,
        count(running_sum_sqft) over (partition by region order by running_min_sqft asc, id asc rows between unbounded preceding and current row) as running_count_running_sum_sqft,
        min(running_sum_sqft) over (partition by region order by running_min_sqft asc, id asc rows between unbounded preceding and current row) as running_min_running_sum_sqft
    from scored
),
with_last as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.order_discount,
        latest.order_order_date
    from with_windows s
    left join lateral (
            select c.discount as order_discount, c.order_date as order_order_date
            from orders c
            where c.store_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) latest on true
),
remaining_keys as (
    -- Set intersection on keys
    select id
    from with_last
    intersect
    select id
    from stores
    where city in ('London', 'Chiang Mai', 'Berlin')
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_last s
    join remaining_keys k on s.id = k.id
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        round(running_count_running_sum_sqft / sum(running_count_running_sum_sqft) over (partition by region), 4) as running_count_running_sum_sqft_share,
        stddev_samp(running_sum_running_sum_sqft) over (partition by region) as region_stddev_samp_running_sum_running_sum_sqft
    from reconciled
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from enriched s
    where not exists (
            select 1
            from orders ch
            where ch.store_id = s.id
              and ch.order_date <= '2024-03-17'
        )
)
-- Grouped metrics
select
    case when running_count_running_sum_sqft_share >= 0.74 then 'PREMIUM' else 'STANDARD' end as running_count_running_sum_sqft_share_tier,
    date_trunc('quarter', opened_date) as quarter_start,
    sum(case when region = 'North' then running_sum_running_sum_sqft else 0 end) as north_running_sum_running_sum_sqft,
    sort_array(collect_set(region)) as region_list,
    percentile_approx(order_discount, 0.9) as order_discount_p90,
    percentile_approx(running_sum_running_sum_sqft, 0.5) as running_sum_running_sum_sqft_p50,
    size(collect_set(region)) as num_regions,
    approx_percentile(running_sum_running_sum_sqft, array(0.25, 0.5, 0.75)) as running_sum_running_sum_sqft_quartiles
from kept
group by case when running_count_running_sum_sqft_share >= 0.74 then 'PREMIUM' else 'STANDARD' end, date_trunc('quarter', opened_date);
