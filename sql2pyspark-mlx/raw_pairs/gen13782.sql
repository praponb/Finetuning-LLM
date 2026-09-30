select o.status, min(p.paid_at) as first_paid_at
from payments p
left outer join orders o on p.order_id = o.id
join customers c on o.customer_id = c.id
where o.status = 'PENDING'
group by o.status
order by o.status desc;
