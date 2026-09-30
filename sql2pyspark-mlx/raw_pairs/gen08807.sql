select
    i.id,
    i.last_restock,
    i.product_id,
    i.store_id,
    s.sqft,
    s.store_name,
    add_months(i.last_restock, 1) last_restock_plus_months
from inventory i
inner join stores s on i.store_id = s.id
where s.sqft < 8431
order by 2;
