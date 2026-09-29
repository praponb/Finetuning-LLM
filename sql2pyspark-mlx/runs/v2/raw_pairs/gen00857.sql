select status
from shipments
where cost >= 76
except
select status
from subscriptions;
