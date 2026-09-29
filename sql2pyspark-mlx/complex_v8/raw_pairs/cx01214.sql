WITH base_holders AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        risk_score,
        joined_on,
        segment,
        CONCAT_WS('|', region, segment) AS region_segment_label,
        CONCAT(region, '#', CAST(id AS STRING)) AS region_ref
    FROM policyholders
    WHERE segment IS NOT NULL
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY risk_score DESC), 3) AS risk_score_percent_rank,
        ROUND(risk_score - AVG(risk_score) OVER w_segment, 2) AS risk_score_vs_segment_avg
    FROM base_holders
    WINDOW w_segment AS (PARTITION BY segment)
),
with_activity AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.id
              AND p.status IS NOT NULL
        )
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM with_activity
    WHERE risk_score > (SELECT MIN(risk_score) FROM with_activity)
)
SELECT *
FROM (
    SELECT joined_on, segment, risk_score
    FROM screened
) src
PIVOT (
    AVG(risk_score)
    FOR segment IN ('SME', 'RETAIL', 'VIP')
)
ORDER BY joined_on
