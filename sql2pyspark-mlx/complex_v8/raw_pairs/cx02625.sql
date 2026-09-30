WITH base AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE start_date < DATE '2025-03-06'
      AND end_date >= DATE '2026-03-13'
),
enriched_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        p.risk_score AS holder_risk_score
    FROM base s
    JOIN policyholders p ON p.id = s.holder_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY holder_risk_score DESC), 3) AS holder_risk_score_percent_rank,
        ROW_NUMBER() OVER w_start_date_month_by_holder_r_2 AS seq_no,
        LAG(premium) OVER (ORDER BY start_date, id) AS prev_premium
    FROM enriched_holder
    WINDOW w_start_date_month_by_holder_r_2 AS (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY holder_risk_score DESC, id)
)
-- Final projection
SELECT
    id,
    holder_id,
    premium,
    start_date,
    end_date,
    holder_risk_score_percent_rank,
    seq_no
FROM with_windows
WHERE premium <= 3544.1
ORDER BY start_date
