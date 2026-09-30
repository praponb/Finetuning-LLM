select
    oi.id,
    oi.order_id,
    o.customer_id,
    o.discount,
    c.credit_limit,
    c.email
from order_items oi
left join orders o on oi.order_id = o.id
left join customers c on o.customer_id = c.id
order by c.credit_limit desc, order_id asc;
