WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(revenue, 0) AS revenue_rounded
    FROM store_sales
    WHERE revenue BETWEEN 1100 AND 2800
)
-- Keep one row per key (latest first)
SELECT *
FROM (
    SELECT *, RANK() OVER (PARTITION BY store_id, product_id ORDER BY revenue NULLS LAST) AS rank_in_group
    FROM cleaned
) t
WHERE rank_in_group = 1
