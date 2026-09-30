SELECT
    id,
    revenue,
    sale_date,
    CASE WHEN revenue >= 4200 THEN 'High' WHEN revenue >= 900 THEN 'Medium' ELSE 'Low' END AS revenue_tier
FROM store_sales
WHERE units >= 27 AND store_id = 3
ORDER BY revenue, id
LIMIT 10
