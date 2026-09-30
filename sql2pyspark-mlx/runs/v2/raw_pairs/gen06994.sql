select
    ss.store_id,
    p.stock_qty,
    s.rating,
    s.country
from store_sales ss
left join products p on ss.product_id = p.id
left join suppliers s on p.supplier_id = s.id
order by country desc;
