select
    i.id,
    i.store_id,
    i.quantity,
    s.region,
    p.stock_qty,
    p.supplier_id
from inventory i
left join stores s on i.store_id = s.id
left join products p on i.product_id = p.id
where i.store_id in (2, 4, 5)
order by supplier_id desc;
