select
    p.id,
    p.launch_date,
    p.brand,
    s.country,
    s.city
from products p
inner join suppliers s on p.supplier_id = s.id;
