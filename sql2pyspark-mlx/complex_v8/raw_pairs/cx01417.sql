WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        DATE_FORMAT(joined_on, 'MMM yyyy') AS joined_on_label,
        CONCAT(segment, '#', CAST(id AS STRING)) AS segment_ref
    FROM policyholders
    WHERE region IN ('North', 'South')
      AND segment = 'RETAIL'
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        holder_name,
        joined_on,
        segment_ref,
        ABS(risk_score - 0.3) AS risk_score_distance,
        LOWER(REGEXP_REPLACE(TRIM(holder_name), ' ', '_')) AS holder_name_slug,
        DATEDIFF(DATE '2025-06-30', joined_on) AS days_since_joined_on
    FROM prepared
    WHERE risk_score >= 0.3
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_name,
        segment_ref,
        MAX(risk_score_distance) OVER (PARTITION BY YEAR(joined_on)) AS joined_on_year_max_risk_score_distance,
        ROUND(days_since_joined_on / NULLIF(SUM(days_since_joined_on) OVER (PARTITION BY YEAR(joined_on)), 0), 4) AS days_since_joined_on_share_of_joined_on_year,
        ROUND(AVG(days_since_joined_on) OVER (ORDER BY risk_score_distance DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS days_since_joined_on_avg_3,
        ROUND(PERCENT_RANK() OVER (ORDER BY risk_score_distance DESC), 3) AS risk_score_distance_percent_rank,
        LAST_VALUE(joined_on) OVER (ORDER BY risk_score_distance DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_joined_on
    FROM annotated
)
SELECT
    id,
    holder_name,
    segment_ref,
    joined_on_year_max_risk_score_distance,
    days_since_joined_on_share_of_joined_on_year,
    last_joined_on
FROM with_windows
ORDER BY days_since_joined_on_share_of_joined_on_year DESC
