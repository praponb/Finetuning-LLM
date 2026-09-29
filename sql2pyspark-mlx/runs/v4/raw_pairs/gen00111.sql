SELECT product_id, MAX(unit_price) - MIN(unit_price) AS unit_price_range FROM order_items GROUP BY product_id ORDER BY unit_price_range DESC;
