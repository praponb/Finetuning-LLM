select
    p.id,
    p.cost,
    p.brand,
    p.price
from products p
where exists (select 1 from inventory i where i.product_id = p.id and i.quantity between 118 and 225);
