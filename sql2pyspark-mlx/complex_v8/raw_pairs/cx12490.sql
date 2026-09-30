WITH scoped_holders AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        segment,
        DATE_ADD(joined_on, 30) AS joined_on_plus_30d,
        LOWER(REGEXP_REPLACE(TRIM(holder_name), ' ', '_')) AS holder_name_slug,
        COALESCE(risk_score, 0.0) AS risk_score_filled
    FROM policyholders
    WHERE risk_score >= 0.3
      AND segment IS NOT NULL
),
latest AS (
    -- Keep the earliest row per region
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        segment,
        joined_on_plus_30d,
        holder_name_slug,
        risk_score_filled
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY region ORDER BY joined_on_plus_30d, id) AS row_num
        FROM scoped_holders
    ) ranked
    WHERE row_num = 1
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(region, 'ALL') AS region,
    COALESCE(segment, 'ALL') AS segment,
    GROUPING(region) AS is_total_region,
    ROUND(SUM(risk_score_filled) / NULLIF(SUM(risk_score), 0), 3) AS risk_score_filled_to_risk_score_ratio,
    COUNT(DISTINCT region) AS distinct_regions
FROM latest
GROUP BY ROLLUP(region, segment)
