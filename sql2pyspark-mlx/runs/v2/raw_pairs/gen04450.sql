SELECT *, CASE WHEN quantity >= 7 THEN 'High' WHEN quantity >= 2 THEN 'Medium' ELSE 'Low' END quantity_band FROM order_items WHERE order_id = 17 AND unit_price >= 140;
