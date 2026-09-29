select shipped_date, 'shipment' as source
from shipments
union all
select end_date as shipped_date, 'subscription' as source
from subscriptions
order by shipped_date;
