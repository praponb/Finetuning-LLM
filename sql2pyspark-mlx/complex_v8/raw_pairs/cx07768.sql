WITH valid_holders AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        risk_score,
        joined_on,
        segment
    FROM policyholders
    WHERE risk_score > 0.2
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        DATE_ADD(joined_on, 30) AS joined_on_plus_30d,
        CONCAT_WS(': ', holder_name, segment) AS holder_name_segment_label,
        COALESCE(segment, 'UNKNOWN') AS segment_or_unknown
    FROM valid_holders
),
segment_stats AS (
    -- Avg risk_score per segment
    SELECT segment, AVG(risk_score) AS avg_risk_score
    FROM enriched
    GROUP BY segment
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_risk_score,
        ROUND(s.risk_score - g.avg_risk_score, 2) AS risk_score_minus_avg
    FROM enriched s
    JOIN segment_stats g ON g.segment = s.segment
    WHERE s.risk_score > g.avg_risk_score
),
labeled AS (
    -- Derive helper columns
    SELECT *, DATE_FORMAT(joined_on, 'yyyy-MM') AS joined_on_label
    FROM vs_group
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT joined_on_plus_30d, segment, risk_score
    FROM labeled
) src
PIVOT (
    SUM(risk_score) AS total, COUNT(risk_score) AS n
    FOR segment IN ('VIP', 'RETAIL')
)
ORDER BY joined_on_plus_30d
