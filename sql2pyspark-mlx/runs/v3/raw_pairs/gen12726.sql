SELECT status, SUM(CASE WHEN status = 'DELIVERED' THEN cost ELSE 0 END) AS delivered_cost, COUNT(DISTINCT cost) AS unique_orders FROM shipments WHERE cost BETWEEN 41 AND 53 GROUP BY status
