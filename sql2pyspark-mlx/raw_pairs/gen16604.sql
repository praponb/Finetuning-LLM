select
    i.id,
    i.last_restock,
    i.store_id,
    p.stock_qty,
    s.store_name,
    s.city
from inventory i
join products p on i.product_id = p.id
join stores s on i.store_id = s.id
