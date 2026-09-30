select order_date, 'order' as source
from orders
where discount between 0.22 and 0.29
union
select start_date, 'subscription' as source
from subscriptions
order by order_date
