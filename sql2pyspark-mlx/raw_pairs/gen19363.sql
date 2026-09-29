WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC, id) AS rank_in_group
        FROM stores
    ) ranked
    WHERE rank_in_group = 1
)
SELECT
    id,
    city,
    region,
    sqft
FROM latest
ORDER BY city DESC, sqft;
