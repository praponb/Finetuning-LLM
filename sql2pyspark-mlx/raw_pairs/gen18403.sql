select *, substring(status, 1, 3) status_prefix
from payments
where status not in ('FAILED', 'REFUNDED')
order by paid_at, id desc;
