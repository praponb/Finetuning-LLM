SELECT id, order_id, amount, CASE WHEN amount >= 1700 THEN 'High' WHEN amount >= 670 THEN 'Medium' ELSE 'Low' END AS amount_level FROM payments WHERE order_id IS NOT NULL ORDER BY amount DESC;
