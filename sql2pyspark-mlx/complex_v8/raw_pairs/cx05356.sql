WITH recent_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix
    FROM policies
    WHERE end_date >= DATE '2026-01-20'
),
with_details AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        s.status_prefix,
        p.holder_name,
        p.risk_score AS holder_risk_score
    FROM recent_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        product,
        premium,
        start_date,
        status_prefix,
        holder_risk_score,
        LAST_VALUE(start_date) OVER (PARTITION BY status ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_start_date,
        RANK() OVER (PARTITION BY status ORDER BY premium) AS premium_rank,
        ROUND(AVG(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date)), 2) AS end_date_month_avg_premium,
        SUM(holder_risk_score) OVER (ORDER BY holder_risk_score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_holder_risk_score,
        MAX(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date)) AS end_date_month_max_premium,
        SUM(premium) OVER (PARTITION BY holder_id ORDER BY holder_risk_score NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        ROUND(CUME_DIST() OVER (ORDER BY holder_risk_score), 3) AS holder_risk_score_cume_dist,
        LEAD(start_date) OVER (ORDER BY holder_risk_score, id) AS next_start_date,
        LAST_VALUE(holder_risk_score) OVER (PARTITION BY holder_id ORDER BY holder_risk_score NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_holder_risk_score,
        RANK() OVER (ORDER BY holder_risk_score, id) AS holder_risk_score_rank,
        ROW_NUMBER() OVER w_status_by_premium AS premium_seq,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date)), 0), 4) AS holder_risk_score_share_of_end_date_month,
        ROUND(AVG(premium) OVER (PARTITION BY holder_id ORDER BY start_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_2_prior,
        MIN(holder_risk_score) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date)) AS end_date_month_min_holder_risk_score
    FROM with_details
    WINDOW w_status_by_premium AS (PARTITION BY status ORDER BY premium, id)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_ym,
        COUNT(*) AS n_rows,
        ROUND(SUM(running_holder_risk_score) / NULLIF(SUM(holder_risk_score_share_of_end_date_month), 0), 3) AS running_holder_risk_score_to_holder_risk_score_s,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list,
        SUM(premium_seq) AS total_premium_seq
    FROM with_trends
    GROUP BY DATE_FORMAT(start_date, 'yyyy-MM')
)
SELECT
    start_date_ym,
    n_rows,
    running_holder_risk_score_to_holder_risk_score_s,
    product_list,
    total_premium_seq
FROM stats
ORDER BY running_holder_risk_score_to_holder_risk_score_s, start_date_ym
LIMIT 10
