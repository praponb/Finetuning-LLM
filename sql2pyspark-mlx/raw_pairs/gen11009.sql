select shipped_date
from shipments
where shipped_date is not null
union all
select end_date
from subscriptions
where end_date is not null;
