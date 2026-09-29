select status
from shipments
union all
select status
from subscriptions
where start_date <= '2025-08-13'
order by status desc;
