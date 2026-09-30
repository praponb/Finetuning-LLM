select
    i.product_id,
    s.store_name,
    s.opened_date,
    p.supplier_id,
    p.price + p.stock_qty as price_plus_stock_qty
from inventory i
join stores s on i.store_id = s.id
join products p on i.product_id = p.id
where p.cost > 560;
