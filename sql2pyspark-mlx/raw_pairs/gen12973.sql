with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        order_id,
        amount,
        paid_at,
        status,
        round(amount * 1.15, 2) amount_scaled,
        coalesce(method, 'NONE') resolved_method
    from payments
    where paid_at >= date '2023-07-13'
      and status = 'FAILED'
)
-- Ranking and gap features
select
    id,
    order_id,
    amount,
    paid_at,
    status,
    amount_scaled,
    resolved_method,
    avg(amount) over w_recent last4_avg_amount,
    max(amount) over w_recent last4_max_amount,
    min(amount) over w_recent last4_min_amount
from filtered
window w_recent as (partition by status order by paid_at, id rows between 3 preceding and current row);
