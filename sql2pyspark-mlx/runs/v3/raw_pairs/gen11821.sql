select p.id, p.amount
from payments p
where p.order_id in (select id from orders where order_date <= '2023-05-08')
