WITH ranked AS (
    SELECT id, region
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date NULLS FIRST, id) AS dedup_rank
        FROM stores
    ) x
    WHERE dedup_rank = 1
)
SELECT id
FROM ranked;
