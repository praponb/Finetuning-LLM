with filtered as (
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        case when status = 'FAILED' then 1 else 0 end is_failed,
        coalesce(method, 'UNKNOWN') method_filled
    from payments
    where status in ('FAILED', 'REFUNDED')
),
enriched as (
    select
        *,
        lag(is_failed, 2) over w_hist prev_is_failed,
        avg(amount) over (partition by order_id) order_id_avg_amount
    from filtered
    window w_hist as (partition by method order by paid_at asc, id asc)
),
joined as (
    select src.*, b.shipping_fee
    from enriched src
    left join orders b
        on src.order_id = b.id
),
enriched_payments as (
    select
        src.*,
        b.status as order_status,
        b.amount as order_amount
    from joined src
    join orders b
        on src.order_id = b.id
),
totals as (
    select
        date_trunc('year', paid_at) as year_start,
        sum(shipping_fee) as total_shipping_fee,
        max(paid_at) as ended_at,
        approx_percentile(order_id_avg_amount, 0.5) as order_id_avg_amount_p50
    from enriched_payments
    group by date_trunc('year', paid_at)
)
select year_start, ended_at
from totals
order by ended_at asc nulls first
