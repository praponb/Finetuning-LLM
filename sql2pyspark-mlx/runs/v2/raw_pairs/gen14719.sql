select id, discount, round(amount * 0.85, 2) as adjusted_amount
from orders
where customer_id = 38 or customer_id in (16, 20, 22, 38)
