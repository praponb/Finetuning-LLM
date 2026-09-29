select status
from accounts
union all
select status
from shipments
where order_id in (5, 9, 19, 25);
