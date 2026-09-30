SELECT device, customer_id
FROM sessions
WHERE customer_id IS NULL
UNION ALL
SELECT device, customer_id
FROM sessions
WHERE device IN ('mobile', 'tablet', 'desktop');
