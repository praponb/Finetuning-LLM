select id, product_id, last_restock
from inventory
where store_id in (1, 2, 4)
union all
select id, product_id, last_restock
from inventory
where last_restock >= '2025-09-12'
order by product_id desc
