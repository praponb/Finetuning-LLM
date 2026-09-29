select
    i.last_restock,
    p.brand,
    p.supplier_id,
    s.rating
from inventory i
inner join products p on i.product_id = p.id
inner join suppliers s on p.supplier_id = s.id
