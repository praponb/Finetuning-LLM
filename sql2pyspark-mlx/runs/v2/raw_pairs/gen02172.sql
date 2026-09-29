with base as (
    select *
    from orders
    where store_id = 25
),
orders_stats as (
    select
        date_trunc('MONTH', order_date) as order_month,
        channel,
        round(stddev(shipping_fee), 2) as stddev_shipping_fee,
        max(order_date) as last_order_date
    from base
    group by date_trunc('MONTH', order_date), channel
)
select
    order_month,
    channel,
    stddev_shipping_fee,
    last_order_date
from orders_stats
order by stddev_shipping_fee, order_month, channel
limit 5;
