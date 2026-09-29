select last_restock, product_id
from inventory
where store_id in (1, 2, 4, 6)
union all
select last_restock, product_id
from inventory
where product_id in (1, 8, 10)
order by last_restock
