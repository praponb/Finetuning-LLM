select
    ss.id,
    ss.store_id,
    ss.revenue,
    p.cost,
    p.product_name,
    p.stock_qty
from store_sales ss
left join products p on ss.product_id = p.id
where ss.units >= 39 and array_contains(p.tags, 'eco');
