WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        opened_date,
        sqft,
        CASE WHEN sqft >= 16234 THEN 'High' WHEN sqft >= 4518 THEN 'Medium' ELSE 'Low' END AS sqft_tier,
        COALESCE(region, 'UNKNOWN') AS resolved_region
    FROM stores
    WHERE opened_date > TO_DATE('2023-03-22')
      AND region IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM valid
    EXCEPT DISTINCT
    SELECT id
    FROM stores
    WHERE LOWER(store_name) LIKE '%ld %'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN remaining_keys k ON s.id = k.id
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.last_restock <= s.opened_date
        )
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        store_name,
        sqft,
        sqft_tier,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY MONTH(opened_date)), 4) AS sqft_share,
        RANK() OVER (ORDER BY sqft DESC NULLS FIRST) AS sqft_rank,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY MONTH(opened_date)), 4) AS sqft_share_2,
        RANK() OVER (ORDER BY sqft DESC NULLS FIRST) AS sqft_rank_2
    FROM kept
    WHERE store_name IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        sqft_tier,
        sqft_share_2,
        sqft_rank_2,
        LEAST(sqft_rank, sqft) AS least_sqft_rank_sqft,
        FLOOR(sqft) AS sqft_whole
    FROM scored
    WHERE sqft_share <= 0.55
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN least_sqft_rank_sqft >= 89 THEN 'PREMIUM' ELSE 'STANDARD' END AS least_sqft_rank_sqft_tier,
    PERCENTILE_APPROX(sqft_share_2, ARRAY(0.25, 0.5, 0.75)) AS sqft_share_2_quartiles,
    ROUND(AVG(sqft_whole), 2) AS avg_sqft_whole,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(sqft_share_2, ARRAY(0.25, 0.5, 0.75)) AS sqft_share_2_quartiles_2,
    APPROX_PERCENTILE(least_sqft_rank_sqft, ARRAY(0.25, 0.5, 0.75)) AS least_sqft_rank_sqft_quartiles,
    APPROX_PERCENTILE(sqft_share_2, 0.9) AS sqft_share_2_p90
FROM cleaned
GROUP BY 1
HAVING COUNT(*) > 1
ORDER BY sqft_share_2_p90 DESC
