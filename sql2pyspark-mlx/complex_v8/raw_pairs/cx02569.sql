WITH filtered_holders AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        SUBSTRING(TRIM(holder_name), 1, 2) AS holder_name_prefix,
        CONCAT_WS(' / ', holder_name, segment) AS holder_name_segment_label,
        CASE WHEN risk_score >= 0.6 THEN 'large' WHEN risk_score >= 0.2 THEN 'medium' ELSE 'small' END AS risk_score_tier
    FROM policyholders
    WHERE segment IS NOT NULL
),
region_stats AS (
    -- Median risk_score per region
    SELECT region, PERCENTILE_APPROX(risk_score, 0.5) AS median_risk_score
    FROM filtered_holders
    GROUP BY region
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_risk_score,
        ROUND(s.risk_score - g.median_risk_score, 2) AS risk_score_minus_median
    FROM filtered_holders s
    JOIN region_stats g ON g.region = s.region
    WHERE s.risk_score >= g.median_risk_score
),
latest AS (
    -- Keep the latest row per region, risk_score_tier
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        holder_name_prefix,
        holder_name_segment_label,
        risk_score_tier,
        median_risk_score,
        risk_score_minus_median
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY region, risk_score_tier ORDER BY joined_on DESC, id DESC) AS rn
        FROM above_average
    ) ranked
    WHERE rn = 1
)
SELECT
    segment,
    region,
    GROUPING_ID() AS grouping_level,
    COUNT(*) FILTER (WHERE LENGTH(TRIM(holder_name)) >= 4) AS n_length_trim_holder_name_4,
    COUNT(*) AS row_count,
    COUNT(DISTINCT risk_score_tier) AS distinct_risk_score_tiers
FROM latest
GROUP BY ROLLUP(segment, region)
