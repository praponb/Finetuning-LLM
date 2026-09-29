select monthly_fee, 'subscription' as source
from subscriptions
union all
select amount, 'payment' as source
from payments;
