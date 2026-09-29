WITH ranked_store_sales AS (
    SELECT
        id,
        store_id,
        units,
        RANK() OVER (PARTITION BY store_id ORDER BY units DESC) AS pos
    FROM store_sales
    WHERE revenue < 2400
)
SELECT id, store_id, units
FROM ranked_store_sales
WHERE pos <= 3;
