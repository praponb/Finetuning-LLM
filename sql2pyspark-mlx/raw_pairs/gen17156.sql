WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, DATE_SUB(opened_date, 81) AS opened_date_minus_81d
    FROM stores
    WHERE opened_date > DATE '2023-07-01'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, LENGTH(city) AS city_length
    FROM valid
    WHERE region IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        store_name,
        region,
        opened_date_minus_81d,
        city_length
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY city_length DESC, id ASC) = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.store_id IN (2, 4)
        )
)
-- Multi-level aggregation
SELECT
    region,
    ROUND(SUM(city_length) / NULLIF(COUNT(CASE WHEN region = 'West' THEN 1 END), 0), 2) AS city_length_per_west,
    AVG(city_length) AS avg_city_length,
    SIZE(COLLECT_SET(region)) AS num_regions,
    GROUPING_ID() AS grouping_id
FROM not_flagged
GROUP BY ROLLUP (region)
HAVING AVG(city_length) >= 0
