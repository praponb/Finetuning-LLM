select status, 'account' as source
from accounts
where balance >= 76000
union all
select status, 'subscription' as source
from subscriptions
where end_date >= '2025-01-03'
order by source;
