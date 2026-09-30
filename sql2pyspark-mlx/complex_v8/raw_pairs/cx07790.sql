WITH scoped AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        CASE WHEN premium >= 3817.5 THEN 'large' WHEN premium >= 2033.8 THEN 'medium' ELSE 'small' END AS premium_tier,
        DATEDIFF(DATE '2025-12-31', start_date) AS days_since_start_date
    FROM policies
    WHERE holder_id <= 8
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.end_date,
        s.status,
        s.premium_tier,
        s.days_since_start_date,
        p.risk_score AS holder_risk_score
    FROM scoped s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        premium,
        end_date,
        status,
        premium_tier,
        days_since_start_date,
        ROUND(days_since_start_date - AVG(days_since_start_date) OVER (PARTITION BY status), 2) AS days_since_start_date_vs_status_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), status ORDER BY days_since_start_date DESC), 3) AS days_since_start_date_cume_dist,
        LAG(end_date, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), status ORDER BY days_since_start_date DESC, id) AS prev2_end_date,
        ROUND(PERCENT_RANK() OVER w_status_by_days_since_start_d, 3) AS days_since_start_date_percent_rank,
        LAG(holder_risk_score, 3) OVER w_all_by_days_since_start_date AS prev3_holder_risk_score,
        MAX(holder_risk_score) OVER (PARTITION BY premium_tier) AS premium_tier_max_holder_risk_score,
        LEAD(days_since_start_date, 2) OVER (ORDER BY holder_risk_score DESC, id) AS next2_days_since_start_date,
        ROUND(AVG(holder_risk_score) OVER (PARTITION BY status), 2) AS status_avg_holder_risk_score,
        FIRST_VALUE(days_since_start_date) IGNORE NULLS OVER (ORDER BY days_since_start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_days_since_start_date,
        COUNT(*) OVER (PARTITION BY premium_tier) AS rows_in_premium_tier,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER w_premium_tier, 0), 4) AS holder_risk_score_share_of_premium_tier,
        FIRST_VALUE(end_date) OVER (ORDER BY holder_risk_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_end_date
    FROM enriched
    WINDOW w_status_by_days_since_start_d AS (PARTITION BY status ORDER BY days_since_start_date, id), w_all_by_days_since_start_date AS (ORDER BY days_since_start_date DESC, id), w_premium_tier AS (PARTITION BY premium_tier)
)
SELECT DISTINCT
    next2_days_since_start_date,
    premium_tier,
    days_since_start_date_cume_dist
FROM metrics
ORDER BY days_since_start_date_cume_dist
