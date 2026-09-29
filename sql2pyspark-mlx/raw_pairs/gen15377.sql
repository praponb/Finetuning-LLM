select id, product_id, store_id
from inventory
where product_id = 3
intersect
select id, product_id, store_id
from inventory
where store_id in (3, 6)
order by product_id
