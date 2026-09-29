SELECT status
FROM accounts
WHERE balance IS NOT NULL
INTERSECT
SELECT status
FROM shipments
WHERE delivered_date BETWEEN '2024-12-01' AND '2025-08-01'
