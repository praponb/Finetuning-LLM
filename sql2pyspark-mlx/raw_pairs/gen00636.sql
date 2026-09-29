with valid as (
    -- Keep recent, valid rows
    select
        id,
        store_id,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        coalesce(channel, 'Unknown') as channel_filled,
        cast(discount as int) as discount_int,
        coalesce(channel, 'NONE') as channel_clean
    from orders
    where order_date > date '2023-07-05'
      and status in ('RETURNED', 'SHIPPED', 'COMPLETED')
)
-- Ranking and gap features
select
    id,
    status,
    amount,
    shipping_fee,
    discount_int,
    channel_clean,
    max(shipping_fee) over (partition by store_id, channel order by shipping_fee asc, id asc rows between unbounded preceding and current row) as running_max_shipping_fee,
    avg(shipping_fee) over (partition by store_id, channel order by shipping_fee asc, id asc rows between unbounded preceding and current row) as running_avg_shipping_fee,
    min(shipping_fee) over (partition by store_id, channel order by shipping_fee asc, id asc rows between unbounded preceding and current row) as running_min_shipping_fee
from valid
