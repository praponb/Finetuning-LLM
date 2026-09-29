select
    p.id,
    p.order_id,
    o.order_date,
    c.name,
    c.email
from payments p
left outer join orders o on p.order_id = o.id
left outer join customers c on o.customer_id = c.id
where o.status in ('COMPLETED', 'SHIPPED') and o.amount between 680 and 1280
order by p.order_id desc;
