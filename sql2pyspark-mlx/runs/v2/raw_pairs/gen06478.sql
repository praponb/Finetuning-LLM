select status, 'order' as source
from orders
where channel in ('mobile', 'web', 'partner')
union
select status, 'subscription' as source
from subscriptions;
