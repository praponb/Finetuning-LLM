select
    i.id,
    i.last_restock,
    p.stock_qty,
    p.brand,
    s.supplier_name,
    p.stock_qty + p.price stock_qty_plus_price
from inventory i
inner join products p on i.product_id = p.id
full outer join suppliers s on p.supplier_id = s.id
order by stock_qty_plus_price;
