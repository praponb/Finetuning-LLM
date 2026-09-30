select product_id, max(unit_price) - min(unit_price) as unit_price_range from order_items where quantity between 5 and 6 and unit_price >= 310 group by product_id order by product_id limit 3;
