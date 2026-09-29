SELECT carrier, SUM(CASE WHEN status = 'RETURNED' THEN cost ELSE 0 END) AS returned_cost
FROM shipments
WHERE cost > 79
GROUP BY carrier
HAVING SUM(CASE WHEN status = 'RETURNED' THEN cost ELSE 0 END) > 22;
