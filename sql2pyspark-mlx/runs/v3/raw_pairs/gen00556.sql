select
    i.product_id,
    p.stock_qty,
    p.category,
    p.supplier_id
from inventory i
join products p on i.product_id = p.id
where p.category = 'Sports' or i.store_id = 4;
