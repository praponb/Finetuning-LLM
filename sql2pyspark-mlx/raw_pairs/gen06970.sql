WITH recent_store_sales AS (
    SELECT *
    FROM store_sales
    WHERE sale_date BETWEEN '2023-05-01' AND '2024-10-13'
),
Agg AS (
    SELECT store_id, COUNT(*) AS store_sale_count, COUNT(*) AS row_count
    FROM recent_store_sales
    GROUP BY store_id
)
SELECT store_id, store_sale_count
FROM Agg
WHERE store_sale_count < 9
