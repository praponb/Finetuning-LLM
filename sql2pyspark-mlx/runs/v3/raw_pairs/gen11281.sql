SELECT store_id, row_count
FROM (
    SELECT store_id, COUNT(*) AS row_count
    FROM store_sales
    WHERE YEAR(sale_date) >= 2025
    GROUP BY store_id
) AS agg
WHERE row_count > 6
ORDER BY store_id DESC, 2 DESC;
