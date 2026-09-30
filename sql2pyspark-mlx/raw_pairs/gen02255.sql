WITH ranked AS (
    SELECT
        id,
        store_id,
        units,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units DESC, id) AS rnk
    FROM store_sales
)
SELECT *
FROM ranked
WHERE rnk <= 3;
