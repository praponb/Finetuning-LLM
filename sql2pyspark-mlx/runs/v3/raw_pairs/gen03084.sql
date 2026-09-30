SELECT id, order_id, product_id, CASE WHEN unit_price >= 390 THEN 'High' WHEN unit_price >= 190 THEN 'Medium' ELSE 'Low' END AS unit_price_tier FROM order_items WHERE product_id IN (3, 5, 37);
