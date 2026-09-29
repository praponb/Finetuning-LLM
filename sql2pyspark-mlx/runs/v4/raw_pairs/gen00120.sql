SELECT product_id, COUNT(DISTINCT unit_price) unique_orders, COUNT(quantity) quantity_count FROM order_items GROUP BY product_id
