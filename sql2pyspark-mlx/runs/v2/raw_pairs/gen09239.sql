select
    p.id,
    p.price,
    p.stock_qty,
    p.supplier_id
from products p
where p.id in (select product_id from store_sales where store_id in (1, 2, 4))
order by supplier_id, p.stock_qty desc;
