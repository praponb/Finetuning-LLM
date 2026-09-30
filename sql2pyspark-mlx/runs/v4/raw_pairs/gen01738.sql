WITH RankedStoreSales AS (
    SELECT
        id,
        store_id,
        revenue,
        product_id,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue, id) AS rnk
    FROM store_sales
)
SELECT *
FROM RankedStoreSales
WHERE rnk <= 3;
