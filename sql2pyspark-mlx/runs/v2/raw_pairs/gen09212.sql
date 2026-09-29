select status
from accounts
where account_type <> 'SAVINGS'
intersect
select status
from subscriptions
