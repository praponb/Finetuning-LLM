SELECT product_id, SUM(quantity * unit_price) revenue, SUM(quantity) total_quantity, MIN(quantity) min_quantity FROM order_items GROUP BY product_id
