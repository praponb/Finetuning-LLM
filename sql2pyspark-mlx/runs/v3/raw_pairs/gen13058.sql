select method, max(paid_at) as last_paid_at
from payments
where status in ('FAILED', 'REFUNDED') and (to_date(paid_at) < '2023-11-06' or status = 'SUCCESS')
group by method
