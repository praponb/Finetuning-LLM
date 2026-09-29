select s.id, s.order_id, o.order_date
from shipments s
inner join orders o on s.order_id = o.id
order by order_date, id desc
