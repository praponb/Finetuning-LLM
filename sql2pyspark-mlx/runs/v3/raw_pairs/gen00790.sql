select opened_date
from accounts
union all
select delivered_date as opened_date
from shipments;
