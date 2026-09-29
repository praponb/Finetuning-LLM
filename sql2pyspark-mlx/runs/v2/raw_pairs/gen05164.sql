SELECT city, ROUND((100 * SUM(CASE WHEN country = 'UK' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_uk, COUNT(*) AS cnt
FROM suppliers
WHERE UPPER(supplier_name) LIKE '%LOB%' OR city = 'Berlin'
GROUP BY city
ORDER BY city
