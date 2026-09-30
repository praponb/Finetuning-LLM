with base as (
    select
        *,
        add_months(sale_date, 12) as sale_date_plus_months,
        floor(units) as units_whole
    from store_sales
    where revenue <= 1500
),
metrics as (
    select
        id,
        store_id,
        product_id,
        sale_date,
        sale_date_plus_months,
        units_whole,
        sum(revenue) over (partition by date_trunc('WEEK', sale_date_plus_months), product_id order by sale_date_plus_months, id rows between unbounded preceding and current row) as running_revenue,
        sum(revenue) over w_recent as running_revenue_2,
        sum(revenue) over (order by sale_date, id rows between unbounded preceding and current row) as running_revenue_3
    from base
    window w_recent as (partition by store_id order by sale_date_plus_months, id rows between unbounded preceding and current row)
),
base_store_sales as (
    select
        id,
        product_id,
        sale_date,
        sale_date_plus_months,
        units_whole,
        running_revenue,
        running_revenue_3,
        running_revenue_2 * units_whole as running_revenue_2_times_units_whole
    from metrics
    where sale_date <= '2023-11-01'
),
with_dims as (
    select
        e.id,
        e.product_id,
        e.sale_date,
        e.sale_date_plus_months,
        e.units_whole,
        e.running_revenue,
        e.running_revenue_3,
        e.running_revenue_2_times_units_whole,
        d.product_name
    from base_store_sales e
    inner join products d
        on e.product_id = d.id
    where d.price >= 450
),
metrics_store_sales as (
    select
        *,
        min(running_revenue) over w_recent as running_min_running_revenue,
        count(running_revenue) over w_recent as running_count_running_revenue,
        max(running_revenue) over w_recent as running_max_running_revenue,
        avg(running_revenue) over w_recent as running_avg_running_revenue,
        max(running_revenue_2_times_units_whole) over (partition by year(sale_date)) as sale_date_year_max_running_revenue_2_times_units_whole
    from with_dims
    window w_recent as (partition by product_id order by running_revenue_2_times_units_whole, id rows between unbounded preceding and current row)
)
select
    id,
    product_id,
    sale_date,
    sale_date_plus_months,
    running_revenue,
    product_name,
    running_min_running_revenue,
    running_count_running_revenue,
    running_max_running_revenue,
    lag(running_revenue_2_times_units_whole) over (partition by product_id order by running_revenue_3, id) as prev_running_revenue_2_times_units_whole,
    lag(running_min_running_revenue) over (partition by product_id order by running_revenue_3, id) as prev_running_min_running_revenue,
    row_number() over (partition by product_id order by running_revenue_3, id) as row_seq
from metrics_store_sales;
