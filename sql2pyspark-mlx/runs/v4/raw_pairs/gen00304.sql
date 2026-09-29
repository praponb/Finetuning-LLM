with valid_orders as (
    select *
    from orders
    where order_date <= '2023-12-06' and store_id = 20
),
orders_stats as (
    select status, round(stddev(amount), 2) as stddev_amount
    from valid_orders
    group by status
)
select status, stddev_amount
from orders_stats
order by stddev_amount desc, status desc;
