WITH filtered AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM policies
    WHERE product NOT IN ('LIFE', 'AUTO')
),
with_details_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.status_or_unknown,
        p.risk_score AS holder_risk_score
    FROM filtered s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status_or_unknown,
        holder_risk_score,
        SUM(premium) OVER (ORDER BY start_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS premium_sum_7,
        LAST_VALUE(start_date) OVER (PARTITION BY holder_id, status_or_unknown ORDER BY holder_risk_score NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_start_date,
        premium - LAG(premium) OVER w_all_by_start_date AS premium_change,
        ROW_NUMBER() OVER w_holder_id_status_or_unknown_2 AS rn,
        NTH_VALUE(start_date, 2) OVER (ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        ROUND(premium / NULLIF(SUM(premium) OVER w_holder_id, 0), 4) AS premium_share_of_holder_id,
        MIN(premium) OVER (PARTITION BY holder_id, status_or_unknown ORDER BY holder_risk_score NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_premium_so_far,
        COUNT(*) OVER w_holder_id AS rows_in_holder_id,
        SIZE(COLLECT_SET(status_or_unknown) OVER w_holder_id) AS distinct_status_or_unknown_in_holder_id
    FROM with_details_holder
    WINDOW w_all_by_start_date AS (ORDER BY start_date, id), w_holder_id_status_or_unknown_2 AS (PARTITION BY holder_id, status_or_unknown ORDER BY holder_risk_score NULLS LAST, id), w_holder_id AS (PARTITION BY holder_id)
),
leaders AS (
    -- Top 3 per group
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status_or_unknown,
        holder_risk_score,
        premium_sum_7,
        last_start_date,
        premium_change,
        second_start_date,
        premium_share_of_holder_id,
        min_premium_so_far,
        rows_in_holder_id,
        distinct_status_or_unknown_in_holder_id
    FROM windowed
    WHERE rn <= 3
      AND min_premium_so_far BETWEEN 2690.0 AND 32570.0
)
-- Final projection
SELECT
    id,
    holder_id,
    premium,
    start_date,
    status_or_unknown,
    holder_risk_score,
    last_start_date,
    premium_change,
    second_start_date,
    premium_share_of_holder_id,
    min_premium_so_far,
    rows_in_holder_id,
    distinct_status_or_unknown_in_holder_id,
    premium_sum_7 + min_premium_so_far AS premium_sum_7_plus_min_premium_so_far,
    GREATEST(holder_risk_score, premium) AS greatest_holder_risk_score_premium
FROM leaders
WHERE premium_share_of_holder_id <= 0.8
ORDER BY premium DESC
