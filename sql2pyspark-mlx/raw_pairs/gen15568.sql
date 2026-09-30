with valid as (
    select
        id,
        store_id,
        order_date,
        channel,
        amount,
        shipping_fee,
        trim(status) as clean_status,
        cast(discount as int) as discount_int,
        coalesce(channel, 'OTHER') as channel_clean
    from orders
    where order_date >= to_date('2023-07-14')
      and order_date >= '2023-07-19'
),
metrics as (
    select
        *,
        lead(discount_int) over (partition by channel, store_id order by order_date, id) as next_discount_int,
        lag(amount) over (partition by channel, store_id order by order_date, id) as prev_amount,
        row_number() over (partition by channel, store_id order by order_date, id) as row_seq
    from valid
    where discount_int > 55
),
metrics_orders as (
    select
        *,
        lead(prev_amount, 1) over (partition by channel order by amount, id) as next_prev_amount,
        sum(shipping_fee) over w_part as order_date_month_sum_shipping_fee,
        sum(next_discount_int) over w_running as cumulative_next_discount_int
    from metrics
    window w_part as (partition by month(order_date), channel), w_running as (order by discount_int, id rows between unbounded preceding and current row)
),
metrics_orders_2 as (
    select
        id,
        store_id,
        order_date,
        channel,
        amount,
        channel_clean,
        prev_amount,
        row_seq,
        lead(next_discount_int) over w_grp as next_next_discount_int,
        lag(order_date_month_sum_shipping_fee) over w_grp as prev_order_date_month_sum_shipping_fee,
        row_number() over w_grp as row_seq_2,
        ntile(4) over w_part as prev_amount_bucket
    from metrics_orders
    window w_grp as (partition by store_id order by row_seq, id), w_part as (partition by channel order by prev_amount, id)
),
with_last as (
    select
        s.*,
        x.shipment_status,
        x.cost
    from metrics_orders_2 s
    join lateral (
            select c.status as shipment_status, c.cost
            from shipments c
            where c.order_id = s.id
              and c.shipped_date <= s.order_date
            order by c.shipped_date desc, c.id desc
            limit 1
        ) x on true
)
select
    id,
    store_id,
    order_date,
    channel,
    channel_clean,
    prev_amount,
    row_seq,
    next_next_discount_int,
    prev_order_date_month_sum_shipping_fee,
    case when channel rlike '^(store|web)$' then 'GROUP_A' when channel rlike '^(mobile|partner)$' then 'CORE' else 'GROUP_B' end as channel_zone
from with_last
order by row_seq desc
