with filtered as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        shipping_fee,
        cast(shipping_fee as int) as shipping_fee_int,
        lower(channel) as channel_lower
    from orders
    where store_id is not null
),
metrics as (
    -- Ranking and gap features
    select
        *,
        sum(amount) over (partition by store_id order by amount, id rows between unbounded preceding and current row) as running_sum_amount,
        avg(amount) over (partition by store_id order by amount, id rows between unbounded preceding and current row) as running_avg_amount,
        max(amount) over (partition by store_id order by amount, id rows between unbounded preceding and current row) as running_max_amount,
        lag(amount, 2) over (partition by channel, status order by shipping_fee_int, id) as prev_amount
    from filtered
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from metrics s
    where not exists (
            select 1
            from shipments c
            where c.order_id = s.id
              and c.shipped_date >= s.order_date
        )
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        store_id,
        channel_lower,
        running_sum_amount,
        running_avg_amount,
        round(running_sum_amount / sum(running_sum_amount) over (partition by customer_id, channel), 4) as running_sum_amount_share,
        lag(shipping_fee_int, 1) over (partition by date_trunc('week', order_date) order by shipping_fee, id) as prev_shipping_fee_int,
        avg(running_max_amount) over (partition by year(order_date), store_id order by shipping_fee, id rows between 5 preceding and 1 preceding) as running_max_amount_avg_5_prior
    from eligible
    where channel = 'web'
)
select
    s.id,
    s.store_id,
    s.channel_lower,
    s.running_sum_amount_share,
    s.running_max_amount_avg_5_prior,
    (select min(o2.shipping_fee) from orders o2 where o2.store_id = s.store_id) as min_order_shipping_fee
from with_windows s
order by running_max_amount_avg_5_prior desc, min_order_shipping_fee desc;
