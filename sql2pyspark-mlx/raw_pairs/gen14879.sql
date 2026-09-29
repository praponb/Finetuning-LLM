WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city ORDER BY sqft DESC) AS dedup_rank
        FROM stores
    ) x
    WHERE dedup_rank = 1
)
SELECT
    id,
    store_name,
    dedup_rank
FROM deduped
ORDER BY id DESC
LIMIT 20;
