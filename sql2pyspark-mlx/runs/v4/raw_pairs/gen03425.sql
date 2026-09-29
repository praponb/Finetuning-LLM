with base as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        day(order_date) as order_date_day,
        substring(status, 1, 4) as status_prefix,
        coalesce(channel, 'UNKNOWN') as channel_clean
    from orders
    where order_date > date '2023-06-13'
      and customer_id in (21, 24, 30, 40)
)
-- Native SQL PIVOT
select *
from (
    select
        customer_id,
        store_id,
        status,
        shipping_fee
    from base
) src
pivot (
    avg(shipping_fee) as value_sum,
    count(1) as n
    for status in ('RETURNED' as returned, 'SHIPPED' as shipped, 'COMPLETED' as completed)
);
