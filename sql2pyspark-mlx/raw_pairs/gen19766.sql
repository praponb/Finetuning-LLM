with valid as (
    select
        id,
        customer_id,
        store_id,
        order_date,
        status,
        amount,
        discount,
        shipping_fee,
        lower(channel) as channel_lower,
        shipping_fee + amount as amount_with_shipping
    from orders
    where order_date >= date '2023-08-14'
),
with_windows as (
    select
        id,
        customer_id,
        store_id,
        status,
        amount,
        discount,
        channel_lower,
        count(amount_with_shipping) over w_running as running_count_amount_with_shipping,
        sum(amount_with_shipping) over w_running as running_sum_amount_with_shipping,
        max(amount_with_shipping) over w_running as running_max_amount_with_shipping,
        avg(amount_with_shipping) over w_running as running_avg_amount_with_shipping
    from valid
    window w_running as (partition by customer_id order by amount, id rows between unbounded preceding and current row)
),
base as (
    select
        id,
        store_id,
        status,
        amount,
        discount,
        running_count_amount_with_shipping,
        running_sum_amount_with_shipping,
        running_max_amount_with_shipping,
        running_avg_amount_with_shipping,
        running_sum_amount_with_shipping / nullif(running_avg_amount_with_shipping, 0) as running_sum_amount_with_shipping_per_running_avg_amount_with_shipping,
        round(running_sum_amount_with_shipping, 0) as running_sum_amount_with_shipping_rounded
    from with_windows
    where discount > 0.3
),
enriched as (
    select
        id,
        store_id,
        status,
        running_count_amount_with_shipping,
        running_sum_amount_with_shipping,
        running_max_amount_with_shipping,
        running_avg_amount_with_shipping,
        running_sum_amount_with_shipping_per_running_avg_amount_with_shipping,
        lag(running_sum_amount_with_shipping) over (partition by store_id, status order by amount, id) as prev_running_sum_amount_with_shipping,
        lead(running_sum_amount_with_shipping) over (partition by store_id, status order by amount, id) as next_running_sum_amount_with_shipping,
        lead(running_sum_amount_with_shipping_per_running_avg_amount_with_shipping) over (partition by store_id, status order by amount, id) as next_running_sum_amount_with_shipping_per_running_avg_amount_with_shipping,
        lag(running_sum_amount_with_shipping_per_running_avg_amount_with_shipping) over (partition by store_id, status order by amount, id) as prev_running_sum_amount_with_shipping_per_running_avg_amount_with_shipping,
        row_number() over (partition by store_id, status order by amount, id) as event_seq
    from base
)
select *
from (
    select
        id,
        store_id,
        running_sum_amount_with_shipping_per_running_avg_amount_with_shipping,
        next_running_sum_amount_with_shipping
    from enriched
) src
unpivot include nulls (
    value for metric_name in (running_sum_amount_with_shipping_per_running_avg_amount_with_shipping, next_running_sum_amount_with_shipping)
) unp
