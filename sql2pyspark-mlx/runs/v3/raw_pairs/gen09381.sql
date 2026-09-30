select case when unit_price >= 330 then 'high' when unit_price >= 90 then 'mid' else 'low' end as unit_price_bucket, count(distinct unit_price) as unique_products
from order_items
where unit_price between 140 and 390 and unit_price between 110 and 180 and product_id in (19, 33, 37, 38)
group by case when unit_price >= 330 then 'high' when unit_price >= 90 then 'mid' else 'low' end
having min(unit_price) >= 260
order by unique_products desc;
