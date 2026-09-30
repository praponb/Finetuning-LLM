select
    i.id,
    i.quantity,
    i.product_id,
    i.last_restock
from inventory i
where i.store_id in (1, 2, 6) and i.product_id in (select id from products where category = 'Electronics')
order by last_restock desc
