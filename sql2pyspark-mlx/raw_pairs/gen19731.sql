SELECT id, product_id, unit_price, AVG(unit_price) OVER (PARTITION BY product_id) AS product_id_avg_unit_price FROM order_items ORDER BY product_id DESC;
