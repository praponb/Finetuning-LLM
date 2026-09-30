SELECT
    id,
    revenue,
    ROUND(units * 0.9, 2) AS units_adjusted,
    DATE_FORMAT(sale_date, 'yyyy-MM-dd') AS sale_date_label
FROM store_sales
WHERE units IS NULL AND sale_date BETWEEN '2023-06-01' AND '2025-03-01'
ORDER BY id
LIMIT 10
