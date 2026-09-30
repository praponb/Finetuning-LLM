SELECT order_id, method, CASE WHEN amount >= 1480 THEN 'High' WHEN amount >= 320 THEN 'Medium' ELSE 'Low' END AS amount_level FROM payments WHERE amount BETWEEN 980 AND 1220
