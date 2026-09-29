WITH ranked_stores AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) AS rnk
    FROM stores
)
SELECT *
FROM ranked_stores
WHERE rnk = 1;
