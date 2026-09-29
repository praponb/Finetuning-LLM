WITH scoped_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status
    FROM policies
    WHERE start_date >= DATE '2024-07-19'
),
joined AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.status,
        p.risk_score AS holder_risk_score,
        p.holder_name
    FROM scoped_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY holder_id, product ORDER BY holder_risk_score NULLS LAST, id), 3) AS holder_risk_score_cume_dist,
        RANK() OVER w_product_holder_id_by_holder AS holder_risk_score_rank,
        MAX(holder_risk_score) OVER (PARTITION BY product, holder_id ORDER BY holder_risk_score, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS holder_risk_score_max_2_prior,
        MAX(holder_risk_score) OVER (PARTITION BY status, holder_id ORDER BY premium DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS holder_risk_score_max_2_prior_2,
        ROUND(CUME_DIST() OVER (ORDER BY start_date DESC, id), 3) AS start_date_cume_dist,
        ROUND(AVG(holder_risk_score) OVER (PARTITION BY status, holder_id ORDER BY premium DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS holder_risk_score_avg_7,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status, product ORDER BY start_date DESC), 3) AS start_date_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY status, product ORDER BY start_date DESC, id) AS rn,
        ROUND(premium - AVG(premium) OVER (PARTITION BY status), 2) AS premium_vs_status_avg,
        LEAD(start_date) OVER w_all_by_start_date AS next_start_date,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER (PARTITION BY product), 0), 4) AS holder_risk_score_share_of_product,
        SUM(holder_risk_score) OVER (PARTITION BY holder_id, product ORDER BY holder_risk_score NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_holder_risk_score
    FROM joined
    WINDOW w_product_holder_id_by_holder AS (PARTITION BY product, holder_id ORDER BY holder_risk_score, id), w_all_by_start_date AS (ORDER BY start_date DESC, id)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_status_holder_risk_score_ran, 3) AS holder_risk_score_max_2_prior_2_cume_dist,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER w_holder_id_next_start_date_mo, 0), 4) AS holder_risk_score_share_of_holder_id_next_start_,
        ROUND(AVG(running_holder_risk_score) OVER w_holder_id_next_start_date_mo, 2) AS holder_id_next_start_date_month_avg_running_hold
    FROM metrics
    WINDOW w_status_holder_risk_score_ran AS (PARTITION BY status, holder_risk_score_rank ORDER BY holder_risk_score_max_2_prior_2), w_holder_id_next_start_date_mo AS (PARTITION BY holder_id, MONTH(next_start_date))
),
grouped AS (
    -- Summarise per group
    SELECT
        rn,
        DATE_TRUNC('MONTH', next_start_date) AS next_start_date_month,
        COUNT(*) FILTER (WHERE status IS NOT NULL) AS n_status_is_not_null,
        COUNT(*) AS row_count,
        COUNT(*) FILTER (WHERE product <> 'HOME') AS n_product_home,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids
    FROM windowed
    WHERE holder_risk_score_share_of_holder_id_next_start_ > 0.2
    GROUP BY rn, DATE_TRUNC('MONTH', next_start_date)
)
SELECT
    rn,
    next_start_date_month,
    n_status_is_not_null,
    n_product_home,
    distinct_holder_ids
FROM grouped
ORDER BY n_status_is_not_null DESC, rn
