select status, 'account' as source
from accounts
where account_type in ('CREDIT', 'SAVINGS')
union
select status, 'subscription' as source
from subscriptions
order by source;
