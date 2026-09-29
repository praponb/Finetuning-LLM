select
    o.id,
    o.order_date,
    s.sqft,
    s.opened_date,
    c.email,
    c.credit_limit,
    trim(s.city) clean_city
from orders o
inner join stores s on o.store_id = s.id
inner join customers c on o.customer_id = c.id
where o.shipping_fee < 26
