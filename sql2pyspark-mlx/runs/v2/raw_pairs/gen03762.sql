select max(amount) as max_amount, avg(amount) as avg_amount
from payments
where not (amount <= 630 and order_id in (29, 33))
order by max_amount desc
