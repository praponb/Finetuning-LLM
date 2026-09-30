select *
from subscriptions
where plan != 'standard'
order by customer_id
