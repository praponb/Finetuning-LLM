WITH metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        region,
        opened_date,
        sqft,
        PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('month', opened_date), city ORDER BY sqft DESC NULLS FIRST) AS sqft_percent_rank,
        LAG(sqft, 2) OVER (PARTITION BY city ORDER BY sqft, id) AS prev_sqft,
        LAG(sqft, 2) OVER (PARTITION BY region ORDER BY opened_date, id) AS prev_sqft_2
    FROM stores
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock <= s.opened_date
        )
),
not_flagged_stores AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock <= s.opened_date
        )
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_FORMAT(opened_date, 'yyyy-MM-dd') AS opened_date_label,
        COALESCE(region, 'UNKNOWN') AS region_clean
    FROM not_flagged_stores
    WHERE region IS NOT NULL
      AND store_name LIKE '%Tow%'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        sqft,
        sqft_percent_rank,
        prev_sqft_2,
        region_clean,
        AVG(sqft) OVER w_hist AS last3_avg_sqft,
        COUNT(sqft) OVER w_hist AS last3_count_sqft,
        MIN(sqft) OVER w_hist AS last3_min_sqft,
        AVG(sqft_percent_rank) OVER w_ordered AS region_avg_sqft_percent_rank
    FROM cleaned
    WINDOW w_hist AS (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY region)
)
-- Grouped metrics
SELECT
    CASE WHEN last3_min_sqft >= 16500 THEN 'PREMIUM' ELSE 'STANDARD' END AS last3_min_sqft_tier,
    CASE WHEN prev_sqft_2 >= 12899 THEN 'LARGE' ELSE 'SMALL' END AS prev_sqft_2_tier,
    CASE WHEN sqft >= 15757 THEN 'LARGE' ELSE 'SMALL' END AS sqft_tier,
    MAX(sqft_percent_rank) AS max_sqft_percent_rank,
    STDDEV_SAMP(sqft) AS sqft_stddev,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN last3_min_sqft >= 16500 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY MAX(sqft_percent_rank) DESC) AS max_sqft_percent_rank_dense_rank
FROM enriched
GROUP BY 1, 2, 3
ORDER BY distinct_ids_2 DESC NULLS FIRST
