select end_date
from subscriptions
where end_date is not null
union
select order_date
from orders
where order_date is not null
order by end_date
