SELECT QUARTER(shipped_date) shipped_quarter, SUM(CASE WHEN carrier = 'UPS' THEN 1 ELSE 0 END) ups_count, MIN(cost) min_cost
FROM shipments
WHERE delivered_date BETWEEN '2023-04-01' AND '2024-01-01' OR status = 'DELIVERED'
GROUP BY 1
ORDER BY min_cost DESC;
