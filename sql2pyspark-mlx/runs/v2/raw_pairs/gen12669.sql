select
    paid_at,
    status,
    amount,
    quarter(paid_at) as paid_at_quarter
from payments
where method in ('paypal', 'bank_transfer') and order_id is null and status = 'FAILED'
