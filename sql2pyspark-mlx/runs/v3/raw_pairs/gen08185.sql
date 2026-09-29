WITH RankedStoreSales AS (
    SELECT
        id,
        product_id,
        revenue,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue DESC, id) pos
    FROM store_sales
)
SELECT id, product_id, revenue
FROM RankedStoreSales
WHERE pos <= 1
ORDER BY id;
