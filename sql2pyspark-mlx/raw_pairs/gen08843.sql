select order_date, amount
from orders
where month(order_date) in (5, 6, 12);
