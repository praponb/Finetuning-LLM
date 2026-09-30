WITH valid AS (
    -- Filter policyholders
    SELECT *
    FROM policyholders
    WHERE risk_score > 0.3
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(risk_score) OVER (PARTITION BY segment) AS segment_sum_risk_score,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', joined_on), region ORDER BY joined_on, id) AS seq_no,
        COUNT(*) OVER (PARTITION BY segment) AS rows_in_segment
    FROM valid
    WINDOW w_segment AS (PARTITION BY segment)
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        segment_sum_risk_score,
        seq_no,
        rows_in_segment,
        CONCAT_WS('|', holder_name, segment) AS holder_name_segment_label,
        ADD_MONTHS(joined_on, 12) AS joined_on_plus_12m
    FROM with_windows
)
-- Final projection
SELECT
    id,
    holder_name,
    region,
    segment,
    segment_sum_risk_score,
    rows_in_segment,
    holder_name_segment_label,
    joined_on_plus_12m,
    DATE_ADD(joined_on_plus_12m, 7) AS joined_on_plus_12m_plus_7d
FROM enriched
ORDER BY region
