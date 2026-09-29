WITH ranked_stores AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id) rn
    FROM stores
)
SELECT
    id,
    region,
    sqft,
    opened_date
FROM ranked_stores
WHERE rn <= 5
ORDER BY sqft DESC, region;
