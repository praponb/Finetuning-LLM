select
    o.discount,
    c.signup_date,
    s.city,
    s.sqft
from orders o
inner join customers c on o.customer_id = c.id
inner join stores s on o.store_id = s.id
