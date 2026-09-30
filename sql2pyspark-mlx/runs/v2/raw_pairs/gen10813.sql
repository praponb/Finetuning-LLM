SELECT product_id, order_id, COUNT(DISTINCT quantity) AS unique_orders FROM order_items GROUP BY 1, 2 HAVING MIN(unit_price) > 190
