select status, month(order_date) as order_month, max(order_date) as last_order_date
from orders
group by status, month(order_date);
