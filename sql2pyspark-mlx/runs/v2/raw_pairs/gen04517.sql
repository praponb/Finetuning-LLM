select
    p.id,
    p.launch_date,
    p.product_name,
    p.category,
    s.city,
    s.rating,
    case when s.city in ('Chiang Mai', 'New York') then 'Priority' else 'Other' end as city_group
from products p
inner join suppliers s on p.supplier_id = s.id
where p.brand = 'Acme'
order by launch_date desc, 7 desc;
