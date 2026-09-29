WITH numbered AS (
    SELECT
        id,
        store_id,
        revenue,
        product_id,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue DESC, id) AS rn
    FROM store_sales
)
SELECT
    id,
    store_id,
    revenue,
    product_id
FROM numbered
WHERE rn <= 5;
