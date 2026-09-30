WITH ranked AS (
    SELECT
        id,
        store_id,
        revenue,
        sale_date,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue DESC, id) AS pos
    FROM store_sales
)
SELECT
    id,
    store_id,
    revenue,
    sale_date
FROM ranked
WHERE pos <= 2
