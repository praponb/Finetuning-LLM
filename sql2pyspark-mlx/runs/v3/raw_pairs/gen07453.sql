select
    ss.id,
    ss.store_id,
    ss.sale_date,
    p.price,
    p.brand,
    s.region,
    s.sqft
from store_sales ss
join products p on p.id = ss.product_id
left join stores s on ss.store_id = s.id
where lower(trim(s.store_name)) = 'airport kiosk'
order by s.sqft desc, id
limit 50;
