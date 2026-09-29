WITH base AS (
    -- Scope the policyholders rows
    SELECT
        id,
        region,
        risk_score,
        joined_on,
        DATEDIFF(DATE '2025-12-31', joined_on) AS days_since_joined_on
    FROM policyholders
    WHERE segment = 'RETAIL'
      AND region = 'South'
),
rollup_base AS (
    -- Group-level totals
    SELECT
        region,
        CASE WHEN days_since_joined_on >= 490 THEN 'large' WHEN days_since_joined_on >= 140 THEN 'medium' ELSE 'small' END AS days_since_joined_on_tier,
        DATE_FORMAT(joined_on, 'yyyy-MM') AS joined_on_ym,
        PERCENTILE_APPROX(days_since_joined_on, 0.5) AS median_days_since_joined_on,
        DATEDIFF(MAX(joined_on), MIN(joined_on)) AS joined_on_span_days,
        ROUND(STDDEV(days_since_joined_on), 2) AS stddev_days_since_joined_on
    FROM base
    WHERE region <> 'West'
    GROUP BY region, CASE WHEN days_since_joined_on >= 490 THEN 'large' WHEN days_since_joined_on >= 140 THEN 'medium' ELSE 'small' END, DATE_FORMAT(joined_on, 'yyyy-MM')
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM rollup_base
    WHERE stddev_days_since_joined_on < (SELECT MAX(stddev_days_since_joined_on) FROM rollup_base)
)
-- Native SQL UNPIVOT (wide to long)
SELECT region, days_since_joined_on_tier, metric_name, metric_value
FROM (
    SELECT region, days_since_joined_on_tier, joined_on_span_days, median_days_since_joined_on
    FROM screened
) src
UNPIVOT (
    metric_value FOR metric_name IN (joined_on_span_days, median_days_since_joined_on)
) unp
