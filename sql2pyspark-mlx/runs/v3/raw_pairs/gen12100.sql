select
    i.id,
    i.store_id,
    i.quantity,
    i.product_id,
    p.cost
from inventory i
inner join products p on i.product_id = p.id
where p.category = 'Sports';
