WITH base_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        DATEDIFF(DATE '2025-06-30', end_date) AS days_since_end_date,
        QUARTER(end_date) AS end_date_quarter
    FROM policies
    WHERE start_date < DATE '2025-05-30'
      AND YEAR(end_date) = 2026
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        holder_id,
        end_date,
        status,
        status_or_unknown,
        days_since_end_date,
        end_date_quarter,
        CONCAT_WS(': ', status, status_or_unknown) AS status_status_or_unknown_label,
        CONCAT_WS(' / ', status_or_unknown, status) AS status_or_unknown_status_label
    FROM base_policys
),
qualified AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM scored s
    WHERE s.holder_id IN (
            SELECT p.holder_id
            FROM policies p
            WHERE p.product = 'TRAVEL'
              AND p.status IN ('ACTIVE', 'LAPSED', 'EXPIRED')
        )
),
with_parent AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        s.status,
        s.days_since_end_date,
        s.end_date_quarter,
        s.status_status_or_unknown_label,
        s.status_or_unknown_status_label,
        p.segment AS holder_segment,
        p.risk_score AS holder_risk_score,
        p.region AS holder_region
    FROM qualified s
    JOIN policyholders p ON p.id = s.holder_id AND p.risk_score < 0.6
),
holder_segment_stats AS (
    -- Max end_date_quarter per holder_segment
    SELECT holder_segment, MAX(end_date_quarter) AS max_end_date_quarter
    FROM with_parent
    GROUP BY holder_segment
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_end_date_quarter,
        ROUND(s.end_date_quarter - g.max_end_date_quarter, 2) AS end_date_quarter_minus_max
    FROM with_parent s
    JOIN holder_segment_stats g ON g.holder_segment = s.holder_segment
    WHERE s.end_date_quarter = g.max_end_date_quarter
)
-- Final projection
SELECT
    id,
    holder_id,
    status,
    days_since_end_date,
    end_date_quarter,
    status_status_or_unknown_label,
    status_or_unknown_status_label,
    holder_segment,
    holder_risk_score,
    end_date_quarter_minus_max
FROM vs_group
ORDER BY end_date_quarter DESC, id
LIMIT 20
