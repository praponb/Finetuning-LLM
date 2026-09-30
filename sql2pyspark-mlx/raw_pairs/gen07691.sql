SELECT sale_date
FROM store_sales
WHERE sale_date IS NOT NULL
UNION ALL
SELECT shipped_date
FROM shipments
WHERE shipped_date IS NOT NULL
ORDER BY sale_date DESC;
