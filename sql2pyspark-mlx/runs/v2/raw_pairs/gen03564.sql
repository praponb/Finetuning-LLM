select
    p.cost,
    p.price,
    p.product_name,
    s.country,
    s.rating,
    s.city
from products p
left join suppliers s on p.supplier_id = s.id
where p.product_name like 'R%'
order by p.product_name desc;
