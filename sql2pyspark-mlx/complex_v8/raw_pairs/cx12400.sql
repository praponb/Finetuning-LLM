WITH cleaned AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status
    FROM policies
    WHERE end_date BETWEEN DATE '2026-01-20' AND DATE '2026-09-23'
      AND status = 'ACTIVE'
),
joined AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.status,
        p.risk_score AS holder_risk_score,
        p.segment AS holder_segment
    FROM cleaned s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY status, holder_segment ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium,
        ROUND(CUME_DIST() OVER (PARTITION BY status ORDER BY start_date DESC, id), 3) AS start_date_cume_dist,
        ROUND(AVG(premium) OVER (PARTITION BY holder_segment), 2) AS holder_segment_avg_premium,
        ROUND(premium - AVG(premium) OVER (PARTITION BY holder_segment), 2) AS premium_vs_holder_segment_avg,
        MAX(premium) OVER (PARTITION BY holder_id) AS holder_id_max_premium,
        SUM(premium) OVER (PARTITION BY status, holder_segment ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        NTILE(3) OVER (PARTITION BY status, holder_segment ORDER BY holder_risk_score ASC, id) AS holder_risk_score_tercile,
        ROUND(CUME_DIST() OVER (PARTITION BY status, holder_segment ORDER BY start_date DESC), 3) AS start_date_cume_dist_2,
        DENSE_RANK() OVER (PARTITION BY status, holder_segment ORDER BY start_date, id) AS start_date_dense_rank,
        SUM(holder_risk_score) OVER (PARTITION BY holder_segment, holder_id ORDER BY holder_risk_score ASC NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS holder_risk_score_sum_7,
        SUM(premium) OVER (PARTITION BY status, holder_segment ORDER BY start_date DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS premium_sum_1_prior,
        COUNT(*) OVER (PARTITION BY status ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(AVG(premium) OVER (PARTITION BY holder_segment, holder_id ORDER BY holder_risk_score ASC NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS premium_avg_3
    FROM joined
),
leaders AS (
    -- First row per group
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        holder_risk_score,
        holder_segment,
        last_known_premium,
        start_date_cume_dist,
        holder_segment_avg_premium,
        premium_vs_holder_segment_avg,
        holder_id_max_premium,
        running_premium,
        holder_risk_score_tercile,
        start_date_cume_dist_2,
        holder_risk_score_sum_7,
        premium_sum_1_prior,
        running_count,
        premium_avg_3
    FROM ranked
    WHERE start_date_dense_rank = 1
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY status, holder_id ORDER BY premium_vs_holder_segment_avg) AS premium_vs_holder_segment_avg_rank,
        LEAD(holder_id_max_premium, 2) OVER (PARTITION BY status ORDER BY last_known_premium, id) AS next2_holder_id_max_premium,
        MIN(holder_risk_score_sum_7) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date)) AS start_date_month_min_holder_risk_score_sum_7,
        RANK() OVER (PARTITION BY status, holder_id ORDER BY premium_vs_holder_segment_avg, id) AS premium_vs_holder_segment_avg_rank_2,
        FIRST_VALUE(premium_sum_1_prior) IGNORE NULLS OVER (ORDER BY holder_id_max_premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium_sum_1_prior,
        SUM(running_count) OVER (ORDER BY holder_id_max_premium, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS running_count_sum_7
    FROM leaders
)
-- Final projection
SELECT
    id,
    holder_id,
    premium,
    start_date,
    status,
    holder_risk_score,
    holder_segment,
    last_known_premium,
    start_date_cume_dist,
    holder_segment_avg_premium,
    premium_vs_holder_segment_avg,
    running_premium,
    holder_risk_score_sum_7,
    premium_sum_1_prior,
    running_count,
    premium_avg_3,
    premium_vs_holder_segment_avg_rank,
    next2_holder_id_max_premium,
    premium_vs_holder_segment_avg_rank_2,
    first_known_premium_sum_1_prior,
    running_count_sum_7,
    COALESCE(running_count, 0) AS running_count_filled,
    ROUND(premium_sum_1_prior * 0.9, 2) AS premium_sum_1_prior_adj
FROM sequenced
ORDER BY running_premium, holder_risk_score_sum_7
