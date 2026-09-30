select
    i.id,
    i.last_restock,
    i.store_id,
    p.cost,
    s.supplier_name,
    datediff(current_date(), i.last_restock) as days_since_last_restock
from inventory i
left join products p on i.product_id = p.id
inner join suppliers s on p.supplier_id = s.id
where i.store_id = 4;
