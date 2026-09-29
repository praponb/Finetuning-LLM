WITH base_holders AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        DATEDIFF(DATE '2025-12-31', joined_on) AS days_since_joined_on
    FROM policyholders
    WHERE (risk_score >= 0.2 OR segment = 'SME')
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(risk_score) OVER (PARTITION BY YEAR(joined_on), region ORDER BY risk_score DESC NULLS FIRST, id) AS prev_risk_score,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', joined_on)) AS rows_in_joined_on_month,
        MIN(days_since_joined_on) OVER w_joined_on_month AS joined_on_month_min_days_since_joined_on,
        LAST_VALUE(days_since_joined_on) IGNORE NULLS OVER (PARTITION BY YEAR(joined_on), region ORDER BY risk_score DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_joined_on,
        SIZE(COLLECT_SET(region) OVER w_joined_on_month) AS distinct_region_in_joined_on_month
    FROM base_holders
    WINDOW w_joined_on_month AS (PARTITION BY DATE_TRUNC('MONTH', joined_on))
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(rows_in_joined_on_month, 2) OVER (PARTITION BY region ORDER BY days_since_joined_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rows_in_joined_on_month,
        LAST_VALUE(days_since_joined_on) IGNORE NULLS OVER (PARTITION BY region ORDER BY days_since_joined_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_joined_on_2
    FROM metrics
)
SELECT
    COALESCE(region, 'ALL') AS region,
    distinct_region_in_joined_on_month,
    MAX(second_rows_in_joined_on_month) AS max_second_rows_in_joined_on_month,
    ROUND(STDDEV(second_rows_in_joined_on_month), 2) AS stddev_second_rows_in_joined_on_month,
    MIN(days_since_joined_on) AS min_days_since_joined_on
FROM with_windows
GROUP BY GROUPING SETS ((region, distinct_region_in_joined_on_month), (region))
