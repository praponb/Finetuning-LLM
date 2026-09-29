select
    ss.id,
    ss.store_id,
    ss.sale_date,
    p.price,
    p.supplier_id,
    s.country
from store_sales ss
inner join products p on ss.product_id = p.id
left join suppliers s on p.supplier_id = s.id
where p.price < 940;
