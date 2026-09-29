WITH ranked_stores AS (
    SELECT
        id,
        region,
        opened_date,
        store_name,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC, id) AS pos
    FROM stores
)
SELECT *
FROM ranked_stores
WHERE pos <= 1
ORDER BY pos DESC
