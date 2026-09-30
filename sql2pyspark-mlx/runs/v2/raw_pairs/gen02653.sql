select
    p.cost,
    p.product_name,
    s.rating,
    s.country
from products p
inner join suppliers s on p.supplier_id = s.id
where upper(s.supplier_name) like '%OKY%'
order by cost desc, p.product_name;
