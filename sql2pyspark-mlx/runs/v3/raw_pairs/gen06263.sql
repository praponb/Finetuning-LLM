SELECT SUM(CASE WHEN status = 'RETURNED' THEN cost ELSE 0 END) AS returned_cost
FROM shipments
