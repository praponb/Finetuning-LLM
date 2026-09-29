select
    o.id,
    o.shipping_fee,
    c.name,
    s.sqft,
    s.region,
    o.amount + o.shipping_fee amount_with_shipping
from orders o
full outer join customers c on o.customer_id = c.id
join stores s on s.id = o.store_id
