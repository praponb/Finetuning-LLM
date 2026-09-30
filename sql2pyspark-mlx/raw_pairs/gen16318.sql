WITH valid AS (
    SELECT
        *,
        CAST(sqft AS DOUBLE) AS sqft_str,
        COALESCE(region, 'OTHER') AS region_clean
    FROM stores
    WHERE region IS NOT NULL
),
not_flagged AS (
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.product_id = 2
        )
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC NULLS FIRST, id ASC) AS rank_in_group
        FROM not_flagged
    ) x
    WHERE rank_in_group = 1
),
cleaned AS (
    SELECT
        id,
        city,
        region,
        opened_date,
        sqft,
        sqft_str,
        LEAST(sqft, sqft_str) AS least_sqft_sqft_str,
        COALESCE(region, 'DIRECT') AS resolved_region
    FROM ranked
    WHERE store_name LIKE '%Old%'
),
metrics AS (
    SELECT
        id,
        region,
        MAX(sqft) OVER w_recent AS last3_max_sqft,
        MIN(sqft) OVER w_recent AS last3_min_sqft,
        COUNT(sqft) OVER w_recent AS last3_count_sqft,
        NTILE(10) OVER w_running AS sqft_str_bucket
    FROM cleaned
    WINDOW w_recent AS (PARTITION BY region, city ORDER BY sqft ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY region, city ORDER BY sqft_str ASC, id ASC)
),
daily AS (
    SELECT
        CASE WHEN last3_count_sqft >= 13 THEN 'PREMIUM' ELSE 'STANDARD' END AS last3_count_sqft_tier,
        SORT_ARRAY(COLLECT_SET(region)) AS region_list,
        SUM(last3_count_sqft) AS total_last3_count_sqft
    FROM metrics
    GROUP BY last3_count_sqft_tier
)
SELECT last3_count_sqft_tier
FROM daily
ORDER BY last3_count_sqft_tier DESC
LIMIT 10;
