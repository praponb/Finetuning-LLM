WITH base AS (
    SELECT *
    FROM inventory
    WHERE product_id IN (4, 5) AND store_id IN (2, 4, 5, 6)
),
quantity_bucket_summary AS (
    SELECT
        CASE WHEN quantity >= 264 THEN 'high' WHEN quantity >= 97 THEN 'mid' ELSE 'low' END AS quantity_bucket,
        store_id,
        MAX(quantity) AS max_quantity,
        ROUND(AVG(quantity), 2) AS avg_quantity
    FROM base
    GROUP BY CASE WHEN quantity >= 264 THEN 'high' WHEN quantity >= 97 THEN 'mid' ELSE 'low' END, store_id
)
SELECT *
FROM quantity_bucket_summary
ORDER BY max_quantity DESC, store_id DESC;
