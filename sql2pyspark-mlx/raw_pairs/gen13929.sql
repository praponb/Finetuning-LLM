select status
from accounts
where status is not null
union
select plan
from subscriptions
where plan is not null;
