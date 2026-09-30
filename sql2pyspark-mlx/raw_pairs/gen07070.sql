select o.id, o.status, count(p.id) as payment_count
from orders o
left join payments p on o.id = p.order_id
group by o.id, o.status
having count(p.id) >= 4
