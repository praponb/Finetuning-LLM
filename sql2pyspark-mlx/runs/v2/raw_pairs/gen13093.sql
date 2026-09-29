select
    p.id,
    p.price,
    p.product_name,
    p.brand
from products p
where not exists (select 1 from inventory i where i.product_id = p.id and i.quantity between 184 and 188)
order by product_name, brand, id
limit 10
