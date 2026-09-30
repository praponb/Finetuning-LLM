select id, order_id, floor(amount) as amount_whole
from payments
where order_id in (16, 17)
order by order_id desc, amount_whole desc
