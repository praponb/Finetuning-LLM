select
    method,
    paid_at,
    order_id,
    round(amount * 0.9, 2) as adjusted_amount,
    date_trunc('month', paid_at) as paid_at_period
from payments
where amount >= 1580 or status = 'REFUNDED'
