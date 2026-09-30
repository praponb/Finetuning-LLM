select
    id,
    order_id,
    amount,
    paid_at,
    max(amount) over (partition by order_id) as order_id_max_amount,
    max(amount) over (partition by month(paid_at), order_id) as paid_at_month_max_amount
from payments
