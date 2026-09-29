SELECT status, order_id, DATE_FORMAT(shipped_date, 'yyyy-MM') AS shipped_month, AVG(cost) AS avg_cost FROM shipments WHERE cost IS NULL GROUP BY status, order_id, shipped_month
