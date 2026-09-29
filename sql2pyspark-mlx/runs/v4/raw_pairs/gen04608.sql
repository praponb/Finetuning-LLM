SELECT
    id,
    units,
    store_id,
    sale_date,
    DATE_ADD(sale_date, 42) AS sale_date_plus_42d,
    CASE WHEN revenue >= 3500 THEN 'High' WHEN revenue >= 2000 THEN 'Medium' ELSE 'Low' END AS revenue_level
FROM store_sales
WHERE sale_date BETWEEN '2023-12-08' AND '2024-04-05'
ORDER BY sale_date
