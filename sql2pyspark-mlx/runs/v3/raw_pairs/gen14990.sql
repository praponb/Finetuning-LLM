select start_date
from subscriptions
where customer_id = 30
except
select order_date
from orders
where month(order_date) in (3, 8, 12)
order by start_date;
