select monthly_fee as value
from subscriptions
union
select amount as value
from orders;
