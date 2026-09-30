select
    p.brand,
    p.category,
    avg(i.quantity) as avg_quantity,
    max(i.last_restock) as latest_last_restock
from inventory i
left outer join products p on i.product_id = p.id
inner join suppliers s on p.supplier_id = s.id
where p.brand not in ('Umbrella', 'Globex', 'Stark')
group by p.brand, p.category
