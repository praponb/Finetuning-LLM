select status
from accounts
union
select status
from subscriptions
order by status;
