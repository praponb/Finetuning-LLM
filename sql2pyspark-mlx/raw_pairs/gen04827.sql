with filtered as (
    -- Filter the raw rows and derive helper columns
    select *, round(revenue * 100, 2) as revenue_scaled
    from store_sales
    where sale_date >= date '2023-04-07'
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        stddev_samp(revenue) over (partition by store_id, product_id) as store_id_stddev_samp_revenue,
        corr(units, revenue) over (partition by store_id, product_id) as corr_units_revenue,
        row_number() over (partition by product_id, store_id order by sale_date desc, id) as event_seq
    from filtered
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        revenue_scaled,
        store_id_stddev_samp_revenue,
        event_seq,
        round(corr_units_revenue / units, 2) as corr_units_revenue_per_units,
        round(revenue * 1.15, 2) as revenue_adjusted
    from with_windows
    where store_id_stddev_samp_revenue <= 3724
),
scored as (
    -- Running and moving aggregates
    select
        id,
        product_id,
        units,
        revenue,
        revenue_scaled,
        store_id_stddev_samp_revenue,
        event_seq,
        rank() over (partition by store_id order by units nulls last) as units_rank,
        first_value(store_id_stddev_samp_revenue) ignore nulls over (partition by product_id order by corr_units_revenue_per_units, id rows between unbounded preceding and current row) as first_known_store_id_stddev_samp_revenue,
        avg(revenue_scaled) over (partition by product_id) as product_id_avg_revenue_scaled,
        sum(revenue_adjusted) over (partition by store_id order by units, id rows between unbounded preceding and current row) as cumulative_revenue_adjusted,
        last_value(revenue_adjusted) ignore nulls over (partition by store_id order by revenue, id rows between unbounded preceding and current row) as last_known_revenue_adjusted
    from base
)
-- Grouped metrics
select
    product_id,
    count(distinct product_id) as distinct_product_ids,
    percentile_approx(units_rank, array(0.25, 0.5, 0.75)) as units_rank_quartiles,
    bit_and(units) as units_bit_and
from scored
group by product_id
order by units_bit_and desc;
