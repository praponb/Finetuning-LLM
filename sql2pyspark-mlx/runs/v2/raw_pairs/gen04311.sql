select status, 'order' as source
from orders
where channel = 'web'
union all
select status, 'account' as source
from accounts
where customer_id in (21, 23);
