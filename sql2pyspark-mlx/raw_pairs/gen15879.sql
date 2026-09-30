WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_FORMAT(opened_date, 'MMM yyyy') opened_date_label,
        COALESCE(region, 'DIRECT') resolved_region
    FROM stores
    WHERE region IS NOT NULL
      AND region IS DISTINCT FROM 'West'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.revenue BETWEEN 1100 AND 1900
        )
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC NULLS FIRST, id) AS latest_rank
    FROM kept
    QUALIFY latest_rank = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(sqft) OVER (PARTITION BY region) region_max_sqft,
        AVG(sqft) OVER (PARTITION BY region, city) region_avg_sqft,
        LEAD(sqft, 1) OVER (PARTITION BY region, city ORDER BY opened_date, id) next_sqft,
        PERCENT_RANK() OVER (PARTITION BY region ORDER BY sqft DESC NULLS FIRST) sqft_percent_rank
    FROM latest
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        region,
        city,
        sqft_percent_rank
    FROM enriched
) src
PIVOT (
    MAX(sqft_percent_rank) AS amount,
    COUNT(1) AS item_count
    FOR city IN ('New York' AS new_york, 'Chiang Mai' AS chiang_mai, 'Berlin' AS berlin)
);
