select status, customer_id, min(order_date) as min_order_date
from orders
group by rollup(status, customer_id)
order by status desc, 2;
