select status
from shipments
where carrier is null
except
select status
from subscriptions
where status in ('ACTIVE', 'PAUSED')
order by status;
