select status
from payments
union
select status
from subscriptions
where status in ('PAUSED', 'ACTIVE', 'EXPIRED')
order by status desc
