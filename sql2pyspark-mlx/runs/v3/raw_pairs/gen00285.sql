select order_date as event_date
from orders
except
select shipped_date as event_date
from shipments
where cost between 40 and 44
order by event_date;
