select status, 'order' as source
from orders
union all
select status, 'account' as source
from accounts
order by source desc
