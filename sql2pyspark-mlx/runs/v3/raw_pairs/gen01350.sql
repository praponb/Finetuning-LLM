SELECT
    category,
    CASE WHEN cost >= 660 THEN 'high' WHEN cost >= 190 THEN 'mid' ELSE 'low' END AS cost_bucket,
    MAX(cost) - MIN(cost) AS cost_range,
    SUM(CASE WHEN category = 'Books' THEN price ELSE 0 END) AS books_price
FROM products
WHERE supplier_id = 40 OR brand IS NULL
GROUP BY category, CASE WHEN cost >= 660 THEN 'high' WHEN cost >= 190 THEN 'mid' ELSE 'low' END
ORDER BY cost_range DESC;
