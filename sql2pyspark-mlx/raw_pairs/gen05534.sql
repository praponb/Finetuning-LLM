WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        country,
        city,
        rating
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating NULLS LAST, id) AS dedup_rank
        FROM suppliers
    ) ranked
    WHERE dedup_rank = 1
)
SELECT
    id,
    country,
    rating,
    CASE WHEN country RLIKE '^(TH|DE)$' THEN 'PRIORITY' ELSE 'CORE' END AS country_zone
FROM ranked;
