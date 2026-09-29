WITH scoped_holders AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        segment,
        LOWER(REGEXP_REPLACE(TRIM(segment), ' ', '_')) AS segment_slug,
        LOWER(REGEXP_REPLACE(TRIM(region), ' ', '_')) AS region_slug,
        LOWER(TRIM(segment)) AS segment_clean
    FROM policyholders
    WHERE risk_score BETWEEN 0.1 AND 0.6
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(risk_score) OVER (ORDER BY risk_score DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS risk_score_sum_3,
        LAST_VALUE(risk_score) IGNORE NULLS OVER (ORDER BY risk_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_risk_score,
        ROUND(risk_score - AVG(risk_score) OVER w_region_segment, 2) AS risk_score_vs_region_segment_avg
    FROM scoped_holders
    WINDOW w_region_segment AS (PARTITION BY region, segment)
),
filtered AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.id
              AND p.start_date >= DATE '2024-05-07'
        )
)
SELECT
    id,
    risk_score,
    region_slug,
    segment_clean,
    last_known_risk_score,
    risk_score_vs_region_segment_avg
FROM filtered
WHERE region = 'North'
ORDER BY last_known_risk_score DESC, risk_score_vs_region_segment_avg NULLS LAST
