SELECT *, CASE WHEN amount >= 1550 THEN 'High' WHEN amount >= 540 THEN 'Medium' ELSE 'Low' END AS amount_level, UPPER(status) AS status_upper
FROM orders
WHERE customer_id IN (3, 11, 26) OR order_date <= '2023-09-05'
ORDER BY status, store_id;
