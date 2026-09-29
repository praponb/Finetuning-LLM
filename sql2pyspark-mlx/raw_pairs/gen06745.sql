select *
from payments
where paid_at < '2025-08-01' and (status in ('REFUNDED', 'FAILED', 'SUCCESS') or order_id is not null);
