select order_id, product_id, avg(quantity) avg_quantity, min(unit_price) min_unit_price from order_items where quantity != 6 group by order_id, product_id;
