select
    i.id,
    i.last_restock,
    i.store_id,
    s.region,
    p.launch_date,
    p.price,
    add_months(i.last_restock, 3) as last_restock_plus_months
from inventory i
inner join stores s on i.store_id = s.id
inner join products p on i.product_id = p.id
where i.store_id in (1, 2, 4, 5)
order by last_restock_plus_months;
