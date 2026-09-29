WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at NULLS LAST, id) AS rank_in_group
        FROM sessions
    ) t
    WHERE rank_in_group = 1
)
SELECT
    id,
    customer_id,
    browser,
    country
FROM ranked
ORDER BY country DESC NULLS LAST, id
