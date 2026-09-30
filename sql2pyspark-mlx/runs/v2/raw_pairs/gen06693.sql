select *
from inventory
where store_id in (1, 3, 4)
order by quantity, product_id desc;
