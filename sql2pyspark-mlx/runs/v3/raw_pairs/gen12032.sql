select cost, 'product' source
from products
where brand not in ('Stark', 'Umbrella')
union
select monthly_fee, 'subscription' source
from subscriptions
order by cost;
