select
    p.supplier_id,
    p.category,
    s.city,
    ceil(p.price) as price_whole
from products p
inner join suppliers s on p.supplier_id = s.id
