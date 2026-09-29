select id, product_id
from inventory
where store_id = 6
union all
select id, product_id
from inventory
where product_id = 5
