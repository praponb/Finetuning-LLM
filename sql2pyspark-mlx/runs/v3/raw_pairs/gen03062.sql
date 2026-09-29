select shipped_date
from shipments
where carrier = 'DHL'
intersect
select end_date
from subscriptions
where status in ('ACTIVE', 'PAUSED', 'CANCELLED');
