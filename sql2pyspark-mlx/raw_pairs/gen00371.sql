SELECT *
FROM (
    SELECT store_id, COUNT(revenue) AS revenue_count
    FROM store_sales
    WHERE revenue >= 2600
    GROUP BY store_id
) AS x
WHERE revenue_count > 9
ORDER BY store_id DESC;
