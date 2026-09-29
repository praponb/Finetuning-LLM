select * from (select product_id, avg(unit_price) as avg_unit_price, round(avg(quantity), 2) as avg_quantity from order_items group by product_id) as s where avg_quantity > 54 order by avg_unit_price
