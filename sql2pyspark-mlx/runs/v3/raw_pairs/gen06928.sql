select status
from payments
where status in ('REFUNDED', 'FAILED', 'SUCCESS')
union
select status
from payments
where hour(paid_at) between 11 and 16
order by status;
