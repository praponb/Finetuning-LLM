select id, status, order_id
from payments
where status = 'REFUNDED'
union
select id, status, order_id
from payments
where order_id in (2, 4, 25)
order by order_id;
