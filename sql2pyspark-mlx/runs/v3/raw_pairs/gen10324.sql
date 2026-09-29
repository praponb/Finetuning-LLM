WITH ranked_stores AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC, id) AS row_num
    FROM stores
    WHERE store_name LIKE '%Centr%'
)
SELECT
    id,
    city,
    opened_date,
    region
FROM ranked_stores
WHERE row_num <= 1;
