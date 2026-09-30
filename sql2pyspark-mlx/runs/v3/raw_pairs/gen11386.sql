SELECT product_id, order_id, COUNT(unit_price) unit_price_count, AVG(quantity) avg_quantity FROM order_items WHERE order_id IN (15, 18, 20) GROUP BY product_id, order_id ORDER BY unit_price_count ASC
