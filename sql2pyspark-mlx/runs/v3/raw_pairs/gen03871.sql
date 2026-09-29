SELECT quantity, product_id, CEIL(unit_price) AS unit_price_whole FROM order_items WHERE quantity > 3 AND (order_id IN (31, 34) OR product_id IN (25, 34)) ORDER BY quantity DESC, product_id DESC;
