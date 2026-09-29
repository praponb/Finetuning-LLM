select
    p.cost,
    i.last_restock,
    i.product_id,
    i.quantity
from products p
left join inventory i on i.product_id = p.id
where i.product_id in (1, 5, 10);
