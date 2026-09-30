WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        supplier_name,
        country
    FROM (
        SELECT *, RANK() OVER (PARTITION BY country, city ORDER BY rating DESC) AS dedup_rank
        FROM suppliers
    ) t
    WHERE dedup_rank = 1
)
-- Multi-level aggregation
SELECT
    country,
    COUNT(*) AS cnt,
    COUNT(*) AS num_rows,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list
FROM ranked
GROUP BY country
HAVING COUNT(*) > 1
ORDER BY num_rows DESC
