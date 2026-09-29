select
    p.id,
    p.cost,
    p.category,
    s.supplier_name,
    upper(p.product_name) as product_name_upper
from products p
join suppliers s on p.supplier_id = s.id
where s.city = 'London'
order by category, s.supplier_name desc, id
limit 50
