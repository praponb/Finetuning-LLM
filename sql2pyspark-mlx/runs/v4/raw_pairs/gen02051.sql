SELECT status, order_id, MIN(delivered_date) AS min_delivered_date FROM shipments GROUP BY 1, 2 HAVING COUNT(DISTINCT cost) > 2
