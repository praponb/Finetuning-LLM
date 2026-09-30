with valid_orders as (
    select *, upper(channel) channel_upper
    from orders
    where discount >= 0.12
),
shipping_fee_bucket_summary as (
    select case when shipping_fee >= 44 then 'high' when shipping_fee >= 7 then 'mid' else 'low' end shipping_fee_bucket, min(order_date) first_order_date
    from valid_orders
    group by case when shipping_fee >= 44 then 'high' when shipping_fee >= 7 then 'mid' else 'low' end
)
select *
from shipping_fee_bucket_summary;
