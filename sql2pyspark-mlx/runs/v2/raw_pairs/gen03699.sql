select product_id, sum(unit_price) as total_unit_price, round(sum(unit_price) * 100.0 / sum(sum(unit_price)) over (), 2) as pct_of_total
from order_items
where product_id = 1
group by product_id
order by product_id desc
limit 5;
