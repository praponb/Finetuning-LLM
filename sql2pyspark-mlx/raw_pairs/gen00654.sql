with valid as (
    -- Keep recent, valid rows
    select
        *,
        array_contains(tags, 'sale') as is_sale,
        coalesce(brand, 'N/A') as brand_filled
    from products
    where launch_date >= date '2023-05-11'
),
enriched as (
    -- Ranking and gap features
    select
        id,
        category,
        cost,
        launch_date,
        avg(cost) over (partition by supplier_id order by stock_qty asc, id asc rows between 5 preceding and current row) as cost_avg_5,
        ntile(5) over (partition by brand order by price asc, id asc) as price_bucket,
        sum(price) over (partition by brand, category) as brand_sum_price
    from valid
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        cost,
        launch_date,
        price_bucket,
        sum(cost) over (partition by category order by cost asc, id asc rows between unbounded preceding and current row) as cumulative_cost,
        min(cost_avg_5) over (partition by category) as category_min_cost_avg_5,
        avg(price_bucket) over (partition by category order by cost asc, id asc rows between 3 preceding and 1 preceding) as price_bucket_avg_3_prior
    from enriched
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from scored s
    where exists (
            select 1
            from order_items x
            where x.product_id = s.id
              and x.quantity = 3
        )
),
kept_products as (
    -- Semi-join via a correlated subquery
    select s.*
    from kept s
    where exists (
            select 1
            from order_items x
            where x.product_id = s.id
              and x.order_id in (6, 17, 18, 31)
        )
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        rank() over (order by price_bucket_avg_3_prior desc nulls first) as price_bucket_avg_3_prior_rank,
        lag(cumulative_cost, 2) over (partition by date_trunc('MONTH', launch_date) order by cumulative_cost asc, id asc) as prev_cumulative_cost,
        lead(cumulative_cost, 1) over (order by category_min_cost_avg_5 asc, id asc) as next_cumulative_cost
    from kept_products
),
totals as (
    -- Multi-level aggregation
    select
        date_trunc('month', launch_date) as activity_month,
        date_trunc('year', launch_date) as year_start,
        percentile_approx(category_min_cost_avg_5, array(0.25, 0.5, 0.75)) as category_min_cost_avg_5_quartiles,
        percentile_approx(cost, array(0.25, 0.5, 0.75)) as cost_quartiles,
        percentile_approx(cumulative_cost, array(0.25, 0.5, 0.75)) as cumulative_cost_quartiles
    from metrics
    group by cube (date_trunc('month', launch_date), date_trunc('year', launch_date))
)
select
    activity_month,
    year_start,
    category_min_cost_avg_5_quartiles,
    cost_quartiles
from totals
order by year_start desc;
