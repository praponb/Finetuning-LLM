WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT id, segment
    FROM customers
    QUALIFY RANK() OVER (PARTITION BY country ORDER BY age DESC NULLS LAST) = 1
)
-- Aggregate with conditional metrics
SELECT
    segment,
    COUNT(*) AS row_count,
    SORT_ARRAY(COLLECT_SET(segment)) AS segment_list,
    COUNT(*) AS row_count_2
FROM ranked
GROUP BY 1
ORDER BY row_count_2 DESC NULLS LAST
