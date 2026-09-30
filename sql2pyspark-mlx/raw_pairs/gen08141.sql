WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        opened_date,
        sqft,
        COALESCE(store_name, 'Unknown') AS store_name_filled,
        COALESCE(region, 'DIRECT') AS resolved_region
    FROM stores
    WHERE opened_date > DATE '2023-09-11'
      AND region IS NOT NULL
),
with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.opened_date,
        s.sqft,
        s.resolved_region,
        latest.status
    FROM valid s
    JOIN LATERAL (
            SELECT c.status
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY sqft, id) AS seq_no,
        AVG(sqft) OVER (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS sqft_avg_6_prior,
        AVG(sqft) OVER (PARTITION BY YEAR(opened_date), status ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS sqft_avg_6_prior_2,
        AVG(sqft) OVER (PARTITION BY DATE_TRUNC('month', opened_date), status) AS opened_date_period_avg_sqft
    FROM with_latest
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        sqft,
        resolved_region,
        status,
        seq_no,
        ROUND(sqft_avg_6_prior_2, 0) AS sqft_avg_6_prior_2_rounded,
        DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date
    FROM metrics
    WHERE sqft = 9672
)
-- Multi-level aggregation
SELECT
    CASE WHEN sqft_avg_6_prior_2_rounded >= 83 THEN 'LARGE' ELSE 'SMALL' END AS sqft_avg_6_prior_2_rounded_tier,
    CASE WHEN sqft >= 13080 THEN 'HIGH' ELSE 'LOW' END AS sqft_tier,
    BIT_AND(sqft) AS sqft_bit_and,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(SUM(sqft_avg_6_prior_2_rounded) / NULLIF(COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END), 0), 2) AS sqft_avg_6_prior_2_rounded_per_completed
FROM cleaned
GROUP BY CASE WHEN sqft_avg_6_prior_2_rounded >= 83 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN sqft >= 13080 THEN 'HIGH' ELSE 'LOW' END
ORDER BY sqft_avg_6_prior_2_rounded_tier;
