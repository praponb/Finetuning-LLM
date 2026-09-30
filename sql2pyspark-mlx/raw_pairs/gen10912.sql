select
    i.id,
    i.last_restock,
    i.quantity,
    p.cost,
    p.brand,
    s.opened_date,
    s.sqft
from inventory i
left join products p on p.id = i.product_id
join stores s on i.store_id = s.id
where i.quantity > 236 and size(p.tags) > 1;
