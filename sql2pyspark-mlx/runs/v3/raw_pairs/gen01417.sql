select
    oi.id,
    oi.product_id,
    oi.unit_price,
    o.order_date,
    c.is_active,
    c.signup_date
from order_items oi
inner join orders o on oi.order_id = o.id
full outer join customers c on o.customer_id = c.id
where o.store_id is not null
order by id
