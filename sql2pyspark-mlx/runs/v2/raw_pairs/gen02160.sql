SELECT ROUND((100 * SUM(CASE WHEN brand = 'Acme' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_acme
FROM products
ORDER BY pct_acme DESC;
