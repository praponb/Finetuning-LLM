select product_id, count(*) as inventory_count
from inventory
group by product_id
order by product_id desc;
