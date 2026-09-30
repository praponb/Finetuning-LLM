select product_id, avg(quantity) as avg_quantity
from inventory
group by product_id;
