select opened_date
from accounts
where month(opened_date) in (7, 10, 11)
union all
select delivered_date
from shipments
where cost >= 80
order by opened_date
