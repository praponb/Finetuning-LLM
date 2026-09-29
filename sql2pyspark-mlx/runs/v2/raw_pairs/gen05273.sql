select product_id, sum(quantity) as total_quantity, round(sum(quantity) * 100.0 / sum(sum(quantity)) over (), 2) as pct_of_total
from inventory
group by product_id
order by total_quantity, product_id
limit 50;
