with valid_orders as (
    select *
    from orders
    where channel in ('web', 'store') and order_date < '2025-02-01'
),
order_year_summary as (
    select year(order_date) as order_year, count(*) as row_count, sum(case when status = 'COMPLETED' then 1 else 0 end) as completed_count
    from valid_orders
    group by year(order_date)
)
select order_year, row_count
from order_year_summary
