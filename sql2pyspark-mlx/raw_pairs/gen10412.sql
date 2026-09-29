select id, store_id, quantity
from inventory
where product_id in (3, 7, 8, 9) and (product_id in (5, 8) or product_id in (1, 2))
order by id;
