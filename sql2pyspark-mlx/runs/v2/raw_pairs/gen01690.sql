WITH ranked_stores AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC, id) AS rnk
    FROM stores
    WHERE region IS NULL
)
SELECT *
FROM ranked_stores
WHERE rnk <= 3;
