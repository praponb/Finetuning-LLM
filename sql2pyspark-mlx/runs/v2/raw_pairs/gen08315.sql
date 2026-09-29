WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units DESC, id) pos
    FROM store_sales
)
SELECT
    id,
    store_id,
    units,
    revenue
FROM ranked
WHERE pos <= 5
ORDER BY store_id DESC;
