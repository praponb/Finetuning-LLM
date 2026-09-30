select order_id, avg(amount) as avg_amount, count(amount) as amount_count
from payments
group by order_id
having count(amount) < 3;
