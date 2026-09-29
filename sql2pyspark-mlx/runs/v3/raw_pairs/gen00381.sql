SELECT *
FROM (
    SELECT store_id, COUNT(revenue) AS revenue_count
    FROM store_sales
    GROUP BY store_id
) AS agg
WHERE revenue_count < 7
ORDER BY revenue_count, store_id
LIMIT 10
