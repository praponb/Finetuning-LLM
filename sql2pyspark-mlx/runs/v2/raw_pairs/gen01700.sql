select product_id, round(stddev(quantity), 2) as stddev_quantity, max(unit_price) - min(unit_price) as unit_price_range
from order_items
where order_id = 22 or order_id in (6, 7, 23, 28)
group by product_id
having count(distinct quantity) >= 4
order by unit_price_range desc, product_id
limit 20
