WITH recent AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment
    FROM policyholders
    WHERE segment <> 'VIP'
),
segment_stats AS (
    -- Avg risk_score per segment
    SELECT segment, AVG(risk_score) AS avg_risk_score
    FROM recent
    GROUP BY segment
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_risk_score,
        ROUND(s.risk_score - g.avg_risk_score, 2) AS risk_score_minus_avg
    FROM recent s
    JOIN segment_stats g ON g.segment = s.segment
    WHERE s.risk_score > g.avg_risk_score
),
with_activity AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM compared s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.id
              AND p.end_date < DATE '2026-08-31'
        )
)
SELECT
    id,
    region,
    risk_score,
    joined_on,
    avg_risk_score,
    risk_score_minus_avg
FROM with_activity
WHERE risk_score >= 0.3
ORDER BY region DESC, id
LIMIT 10
