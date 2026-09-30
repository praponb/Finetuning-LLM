select
    o.id,
    o.channel,
    o.discount,
    o.shipping_fee,
    c.age,
    c.is_active,
    c.credit_limit
from orders o
join customers c on o.customer_id = c.id
order by age desc, id
limit 20;
