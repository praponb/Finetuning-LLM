WITH ranked_stores AS (
    SELECT
        id,
        city,
        sqft,
        opened_date,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft, id) AS rnk
    FROM stores
)
SELECT *
FROM ranked_stores
WHERE rnk = 1
ORDER BY city
