select
    ss.id,
    ss.sale_date,
    ss.store_id,
    s.sqft,
    s.region,
    p.stock_qty
from store_sales ss
left join stores s on ss.store_id = s.id
full outer join products p on ss.product_id = p.id
order by sqft desc;
