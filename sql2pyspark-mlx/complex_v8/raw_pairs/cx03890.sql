WITH scoped_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE product NOT IN ('HOME', 'HEALTH')
),
joined_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        p.risk_score AS holder_risk_score
    FROM scoped_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY product, YEAR(end_date) ORDER BY premium NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium,
        SUM(premium) OVER (ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        MAX(holder_risk_score) OVER (PARTITION BY product, holder_id ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_holder_risk_score_so_far,
        ROUND(holder_risk_score - AVG(holder_risk_score) OVER (PARTITION BY holder_id, MONTH(start_date)), 2) AS holder_risk_score_vs_holder_id_start_date_month_,
        DENSE_RANK() OVER (ORDER BY premium, id) AS premium_dense_rank,
        MAX(premium) OVER (PARTITION BY YEAR(start_date) ORDER BY holder_risk_score DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS premium_max_2_prior,
        MIN(premium) OVER w_end_date_month_holder_id AS end_date_month_holder_id_min_premium,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), holder_id), 0), 4) AS premium_share_of_end_date_month_holder_id,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY YEAR(start_date) ORDER BY holder_risk_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        ROUND(AVG(premium) OVER (PARTITION BY YEAR(start_date) ORDER BY holder_risk_score DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS premium_avg_3
    FROM joined_holder
    WINDOW w_end_date_month_holder_id AS (PARTITION BY DATE_TRUNC('MONTH', end_date), holder_id)
),
top_per_group AS (
    -- Top 2 per group
    SELECT *
    FROM windowed
    WHERE premium_dense_rank <= 2
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    start_date,
    holder_risk_score,
    last_known_premium,
    running_premium,
    max_holder_risk_score_so_far,
    premium_dense_rank,
    premium_share_of_end_date_month_holder_id,
    second_end_date,
    premium_avg_3
FROM top_per_group
ORDER BY max_holder_risk_score_so_far DESC
