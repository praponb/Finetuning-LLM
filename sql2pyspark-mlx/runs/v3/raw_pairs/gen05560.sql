select id, amount, round(amount, 0) as amount_rounded
from payments
where order_id in (7, 40) and paid_at >= '2025-03-11';
