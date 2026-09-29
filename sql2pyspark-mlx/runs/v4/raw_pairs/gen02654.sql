select status
from payments
where hour(paid_at) between 11 and 18
union all
select status
from shipments;
