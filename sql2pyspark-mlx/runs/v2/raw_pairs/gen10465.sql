select status
from accounts
where balance > 66000
union all
select status
from shipments
where status = 'RETURNED';
