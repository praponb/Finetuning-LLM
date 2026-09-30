select
    i.id,
    i.quantity,
    i.product_id,
    p.launch_date,
    s.country
from inventory i
left join products p on i.product_id = p.id
inner join suppliers s on p.supplier_id = s.id;
