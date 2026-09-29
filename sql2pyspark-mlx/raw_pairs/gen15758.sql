WITH numbered AS (
    SELECT
        id,
        store_id,
        units,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units DESC, id) AS rn
    FROM store_sales
)
SELECT id, store_id, units
FROM numbered
WHERE rn = 1
