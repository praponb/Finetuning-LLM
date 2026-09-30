WITH prepared_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATE_FORMAT(start_date, 'yyyy-MM-dd') AS start_date_label
    FROM policies
    WHERE (YEAR(end_date) = 2026 OR YEAR(start_date) = 2024)
),
combined_details AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.status,
        s.start_date_label,
        p.risk_score AS holder_risk_score
    FROM prepared_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        product,
        start_date,
        holder_risk_score,
        ROW_NUMBER() OVER (PARTITION BY product ORDER BY holder_risk_score DESC, id) AS rn,
        SUM(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        SUM(holder_risk_score) OVER w_holder_id AS holder_id_sum_holder_risk_score,
        MAX(premium) OVER (PARTITION BY product, DATE_TRUNC('MONTH', start_date)) AS product_start_date_month_max_premium,
        ROUND(premium / NULLIF(SUM(premium) OVER w_status, 0), 4) AS premium_share_of_status,
        NTILE(4) OVER w_product_by_holder_risk_score AS holder_risk_score_quartile,
        NTH_VALUE(start_date, 2) OVER (ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        FIRST_VALUE(start_date) IGNORE NULLS OVER (ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER (PARTITION BY holder_id), 0), 4) AS holder_risk_score_share_of_holder_id,
        MIN(premium) OVER (PARTITION BY product, DATE_TRUNC('MONTH', start_date)) AS product_start_date_month_min_premium,
        NTH_VALUE(premium, 2) OVER (PARTITION BY status, product ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        MAX(premium) OVER (ORDER BY start_date DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS premium_max_2
    FROM combined_details
    WINDOW w_holder_id AS (PARTITION BY holder_id), w_status AS (PARTITION BY status), w_product_by_holder_risk_score AS (PARTITION BY product ORDER BY holder_risk_score DESC, id)
)
-- Final projection
SELECT
    id,
    start_date,
    holder_risk_score,
    rn,
    running_premium,
    holder_id_sum_holder_risk_score,
    product_start_date_month_max_premium,
    holder_risk_score_quartile,
    second_start_date,
    first_known_start_date,
    holder_risk_score_share_of_holder_id,
    product_start_date_month_min_premium,
    second_premium,
    premium_max_2
FROM with_windows
WHERE second_start_date IS NOT NULL
ORDER BY premium_max_2 DESC
