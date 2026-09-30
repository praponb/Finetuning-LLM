select product_id, sum(quantity) total_quantity, round(avg(quantity), 2) avg_quantity
from order_items
group by product_id
having avg_quantity > 37
order by avg_quantity desc;
