select order_id, min(unit_price) as min_unit_price, count(distinct unit_price) as unique_products
from order_items
where unit_price between 140 and 390 and unit_price between 110 and 180 and product_id in (19, 33, 37, 38)
group by order_id
having avg(unit_price) < 350
order by min_unit_price, unique_products desc;
