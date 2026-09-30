with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        product_name,
        category,
        brand,
        cost,
        launch_date,
        supplier_id,
        substring(brand, 1, 4) as brand_prefix,
        round(price, 1) as price_rounded,
        coalesce(brand, 'NONE') as brand_filled
    from products
    where price < 340
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        product_name,
        category,
        launch_date,
        supplier_id,
        brand_prefix,
        price_rounded,
        brand_filled,
        stddev_samp(cost) over (partition by brand) as brand_stddev_samp_cost,
        sum(cost) over (partition by year(launch_date) order by cost, id rows between unbounded preceding and current row) as cumulative_cost,
        avg(price_rounded) over (partition by brand order by cost, id rows between 5 preceding and current row) as price_rounded_avg_5
    from cleaned
    where brand = 'Umbrella'
),
filtered as (
    -- Keep recent, valid rows
    select *, datediff(current_date(), launch_date) as days_since_launch_date
    from with_windows
    where supplier_id is not null
      and cumulative_cost > 1485
),
with_latest as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.category,
        s.launch_date,
        s.brand_prefix,
        s.price_rounded,
        s.brand_filled,
        s.brand_stddev_samp_cost,
        s.cumulative_cost,
        s.days_since_launch_date,
        latest.order_item_unit_price,
        latest.quantity
    from filtered s
    left join lateral (
            select c.unit_price as order_item_unit_price, c.quantity
            from order_items c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) latest on true
),
scored as (
    -- Ranking and gap features
    select
        *,
        lag(brand_stddev_samp_cost, 1) over (partition by category order by order_item_unit_price, id) as prev_brand_stddev_samp_cost,
        sum(cumulative_cost) over (order by days_since_launch_date, id rows between unbounded preceding and current row) as running_cumulative_cost,
        avg(days_since_launch_date) over (partition by category order by launch_date, id rows between 6 preceding and current row) as days_since_launch_date_avg_6,
        count(days_since_launch_date) over (partition by category) as category_count_days_since_launch_date,
        row_number() over (partition by month(launch_date), category order by brand_stddev_samp_cost desc, id) as event_seq
    from with_latest
),
scored_products as (
    -- Running and moving aggregates
    select
        id,
        launch_date,
        brand_prefix,
        price_rounded,
        brand_filled,
        cumulative_cost,
        quantity,
        prev_brand_stddev_samp_cost,
        sum(cumulative_cost) over w_running as running_sum_cumulative_cost,
        min(cumulative_cost) over w_running as running_min_cumulative_cost,
        count(cumulative_cost) over w_running as running_count_cumulative_cost
    from scored
    window w_running as (partition by category order by price_rounded, id rows between unbounded preceding and current row)
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        running_sum_cumulative_cost,
        price_rounded,
        running_min_cumulative_cost
    from scored_products
) src
unpivot include nulls (
    val for metric in (running_sum_cumulative_cost, price_rounded, running_min_cumulative_cost)
)
