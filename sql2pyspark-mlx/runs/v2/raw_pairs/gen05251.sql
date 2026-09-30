select
    i.id,
    i.store_id,
    s.city,
    p.product_name,
    p.price,
    upper(s.store_name) as store_name_upper
from inventory i
inner join stores s on s.id = i.store_id
inner join products p on p.id = i.product_id
where s.city = 'London' and i.product_id in (5, 10);
