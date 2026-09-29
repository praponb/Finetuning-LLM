WITH prepared AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        CONCAT(product, '#', CAST(id AS STRING)) AS product_ref,
        TRUNC(end_date, 'MM') AS end_date_month_start
    FROM policies
    WHERE product NOT IN ('AUTO', 'LIFE')
      AND end_date BETWEEN DATE '2025-04-17' AND DATE '2026-09-23'
),
enriched AS (
    -- Attach holder attributes
    SELECT
        s.*,
        p.region AS holder_region,
        p.segment AS holder_segment,
        p.risk_score AS holder_risk_score
    FROM prepared s
    JOIN policyholders p ON p.id = s.holder_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        premium,
        start_date,
        product_ref,
        holder_region,
        holder_segment,
        ROW_NUMBER() OVER (PARTITION BY status, holder_id ORDER BY start_date DESC, id) AS rn,
        FIRST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY status, holder_id ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium,
        LAG(holder_risk_score) OVER (PARTITION BY holder_segment ORDER BY end_date_month_start DESC, id) AS prev_holder_risk_score,
        MIN(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), holder_segment) AS start_date_month_holder_segment_min_premium,
        SUM(holder_risk_score) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), holder_segment) AS start_date_month_holder_segment_sum_holder_risk_,
        SUM(holder_risk_score) OVER (PARTITION BY holder_segment ORDER BY end_date_month_start DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS holder_risk_score_sum_2_prior,
        ROW_NUMBER() OVER w_holder_segment_by_end_date_m AS end_date_month_start_seq,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_id, status ORDER BY holder_risk_score ASC, id), 3) AS holder_risk_score_percent_rank,
        COUNT(*) OVER (PARTITION BY holder_segment ORDER BY end_date_month_start DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(AVG(premium) OVER w_start_date_month_holder_segm, 2) AS start_date_month_holder_segment_avg_premium,
        DENSE_RANK() OVER (PARTITION BY status, holder_id ORDER BY start_date DESC, id) AS start_date_dense_rank,
        COUNT(*) OVER w_start_date_month_holder_segm AS rows_in_start_date_month_holder_segment
    FROM enriched
    WINDOW w_holder_segment_by_end_date_m AS (PARTITION BY holder_segment ORDER BY end_date_month_start DESC, id), w_start_date_month_holder_segm AS (PARTITION BY DATE_TRUNC('MONTH', start_date), holder_segment), w_holder_id_status_by_holder_r AS (PARTITION BY holder_id, status ORDER BY holder_risk_score ASC, id)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        premium,
        start_date,
        product_ref,
        holder_region,
        holder_segment,
        rn,
        prev_holder_risk_score,
        start_date_month_holder_segment_min_premium,
        start_date_month_holder_segment_sum_holder_risk_,
        holder_risk_score_sum_2_prior,
        holder_risk_score_percent_rank,
        start_date_month_holder_segment_avg_premium,
        start_date_dense_rank,
        rows_in_start_date_month_holder_segment,
        ROUND(AVG(premium) OVER (ORDER BY start_date DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS premium_avg_2,
        RANK() OVER (ORDER BY start_date DESC) AS start_date_rank,
        ROUND(AVG(premium) OVER (PARTITION BY rn), 2) AS rn_avg_premium,
        first_known_premium - LAG(first_known_premium) OVER (ORDER BY start_date_month_holder_segment_avg_premium DESC NULLS FIRST, id) AS first_known_premium_change,
        SUM(running_count) OVER w_rn AS rn_sum_running_count,
        MIN(first_known_premium) OVER (PARTITION BY rn) AS rn_min_first_known_premium
    FROM with_trends
    WINDOW w_rn AS (PARTITION BY rn)
)
SELECT
    id,
    premium,
    start_date,
    product_ref,
    holder_region,
    holder_segment,
    prev_holder_risk_score,
    start_date_month_holder_segment_min_premium,
    start_date_month_holder_segment_sum_holder_risk_,
    holder_risk_score_sum_2_prior,
    start_date_month_holder_segment_avg_premium,
    start_date_dense_rank,
    rows_in_start_date_month_holder_segment,
    premium_avg_2,
    start_date_rank,
    rn_avg_premium,
    rn_min_first_known_premium
FROM ranked
WHERE holder_segment = 'RETAIL'
ORDER BY holder_segment DESC, id
LIMIT 10
