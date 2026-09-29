WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(city) AS city_length,
        COALESCE(region, 'NONE') AS resolved_region
    FROM stores
    WHERE opened_date > DATE '2023-03-29'
      AND region IS NOT NULL
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.last_restock < s.opened_date
        )
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        city,
        region,
        opened_date,
        sqft,
        resolved_region,
        SUM(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
        AVG(city_length) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS city_length_avg_3
    FROM not_flagged
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        city,
        region,
        opened_date,
        resolved_region,
        cumulative_sqft,
        CONCAT(region, ' ', city) AS region_city,
        COALESCE(region, 'NONE') AS region_clean
    FROM enriched
    WHERE region IS NOT NULL
      AND region = 'Central'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        resolved_region,
        LAG(cumulative_sqft) OVER (PARTITION BY city ORDER BY cumulative_sqft, id) AS prev_cumulative_sqft,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY cumulative_sqft, id) AS seq_no,
        FIRST_VALUE(region) IGNORE NULLS OVER (ORDER BY cumulative_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_region
    FROM filtered
)
SELECT
    id,
    city,
    resolved_region,
    prev_cumulative_sqft,
    first_known_region
FROM metrics
ORDER BY city NULLS LAST, id DESC
LIMIT 20;
