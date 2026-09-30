select status, 'account' as source
from accounts
where opened_date >= '2023-11-18'
union
select status, 'subscription' as source
from subscriptions
