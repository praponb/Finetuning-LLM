select status
from accounts
union
select status
from subscriptions
where plan = 'premium'
order by status
