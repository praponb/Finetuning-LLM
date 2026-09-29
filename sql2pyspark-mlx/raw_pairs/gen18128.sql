with valid as (
    select
        *,
        to_date(paid_at) paid_at_date,
        round(amount, 2) amount_rounded
    from payments
    where status = 'SUCCESS'
),
kept_keys as (
    select id
    from valid
    intersect all
    select id
    from payments
    where hour(paid_at) between 11 and 16
),
kept_rows as (
    select s.*
    from valid s
    inner join kept_keys k on s.id = k.id
),
joined as (
    select a.*, o.status as order_status
    from kept_rows a
    left join orders o
        on a.order_id = o.id
),
metrics as (
    select
        *,
        sum(amount) over (partition by method) method_sum_amount,
        stddev_samp(amount_rounded) over (partition by order_id) order_id_stddev_samp_amount_rounded,
        avg(amount_rounded) over (order by cast(paid_at as bigint) asc range between 2592000 preceding and current row) amount_rounded_avg_30d
    from joined
),
cleaned as (
    select
        *,
        initcap(trim(order_status)) order_status_title,
        coalesce(order_status, 'UNKNOWN') resolved_order_status
    from metrics
    where paid_at > to_timestamp('2023-10-13 13:00:00')
),
with_dims as (
    select
        src.order_id,
        src.method,
        src.amount,
        src.paid_at,
        src.status,
        src.paid_at_date,
        src.amount_rounded,
        src.order_status,
        src.amount_rounded_avg_30d,
        src.order_status_title,
        src.resolved_order_status,
        o.channel,
        o.amount as order_amount
    from cleaned src
    left join orders o
        on src.order_id = o.id
)
select *
from (
    select
        status,
        channel,
        order_amount
    from with_dims
) src
pivot (
    sum(order_amount)
    for channel in ('partner', 'web')
);
