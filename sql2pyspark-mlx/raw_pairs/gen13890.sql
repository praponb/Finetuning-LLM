select
    oi.product_id,
    oi.quantity,
    p.launch_date,
    s.supplier_name,
    s.city
from order_items oi
inner join products p on p.id = oi.product_id
left outer join suppliers s on p.supplier_id = s.id
order by supplier_name, s.city;
