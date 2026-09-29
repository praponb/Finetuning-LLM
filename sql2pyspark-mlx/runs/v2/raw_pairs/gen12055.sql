select product_id, round(stddev(unit_price), 2) as stddev_unit_price from order_items where unit_price > 280 and quantity != 6 and unit_price <= 230 group by product_id
