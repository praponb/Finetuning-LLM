select delivered_date as event_date
from shipments
where order_id is not null
intersect
select opened_date as event_date
from accounts
where month(opened_date) in (6, 10, 12)
order by event_date;
