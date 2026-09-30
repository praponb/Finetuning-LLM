WITH top_stores AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date, id) AS rnk
    FROM stores
)
SELECT id, city, opened_date
FROM top_stores
WHERE rnk = 1
ORDER BY city;
