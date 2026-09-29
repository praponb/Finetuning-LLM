select
    ss.id,
    ss.units,
    p.stock_qty,
    p.product_name,
    s.store_name
from store_sales ss
inner join products p on p.id = ss.product_id
left join stores s on ss.store_id = s.id
order by stock_qty, id
limit 1
