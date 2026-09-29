WITH top_store_sales AS (
    SELECT
        id,
        store_id,
        revenue,
        units,
        DENSE_RANK() OVER (PARTITION BY store_id ORDER BY revenue DESC) rnk
    FROM store_sales
)
SELECT
    id,
    store_id,
    revenue,
    units
FROM top_store_sales
WHERE rnk <= 2;
