select 'product' as kind, cost as value
from products
where cost is not null
union all
select 'subscription' as kind, monthly_fee as value
from subscriptions
where monthly_fee is not null
order by value desc
