select order_id, sum(amount) total_amount, count(amount) amount_count
from payments
group by order_id;
