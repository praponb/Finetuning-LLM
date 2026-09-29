select 'subscription' as kind, plan
from subscriptions
where plan is not null
union
select 'transaction' as kind, merchant
from transactions
where merchant is not null;
