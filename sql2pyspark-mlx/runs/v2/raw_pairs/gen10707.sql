select
    ss.id,
    ss.units,
    ss.product_id,
    p.brand,
    s.supplier_name,
    s.city
from store_sales ss
left outer join products p on ss.product_id = p.id
join suppliers s on p.supplier_id = s.id
where p.category in ('Home', 'Books', 'Fashion')
order by brand desc, city, id
limit 10
