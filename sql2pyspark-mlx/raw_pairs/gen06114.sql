SELECT carrier, MAX(cost) AS max_cost, SUM(CASE WHEN carrier = 'UPS' THEN cost ELSE 0 END) AS ups_cost
FROM shipments
GROUP BY carrier
