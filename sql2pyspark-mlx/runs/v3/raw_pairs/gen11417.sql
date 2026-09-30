select
    p.category,
    p.cost,
    p.brand,
    i.product_id,
    i.last_restock
from products p
left join inventory i on i.product_id = p.id
where i.store_id in (2, 4, 5) and i.store_id in (3, 4, 5)
