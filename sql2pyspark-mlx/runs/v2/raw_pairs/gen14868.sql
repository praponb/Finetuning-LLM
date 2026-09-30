select id, amount, status
from payments
where (order_id = 34 and amount >= 560) or paid_at < '2025-02-15';
