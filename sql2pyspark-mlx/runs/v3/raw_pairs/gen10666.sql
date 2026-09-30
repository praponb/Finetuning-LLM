select
    ss.id,
    ss.store_id,
    p.stock_qty,
    s.city,
    s.supplier_name
from store_sales ss
join products p on ss.product_id = p.id
join suppliers s on p.supplier_id = s.id
where s.rating is not null
order by 5 asc;
