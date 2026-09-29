select cost, 'product' as source
from products
union all
select monthly_fee as cost, 'subscription' as source
from subscriptions
order by cost
