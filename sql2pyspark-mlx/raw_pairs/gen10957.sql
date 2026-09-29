select s.id, s.cost, s.order_id
from shipments s
where s.status = 'RETURNED' and s.order_id in (select id from orders where order_date >= '2025-04-01')
order by s.id asc
