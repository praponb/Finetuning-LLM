SELECT cost, DATEDIFF(CURRENT_DATE(), shipped_date) / 365 AS years_since_shipped_date
FROM shipments
WHERE status IN ('DELIVERED', 'LOST');
