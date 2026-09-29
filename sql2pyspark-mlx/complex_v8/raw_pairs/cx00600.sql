WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        CASE WHEN product IN ('TRAVEL', 'LIFE') THEN 'group_a' WHEN product = 'HOME' THEN 'group_b' ELSE 'other' END AS product_group
    FROM policies
    WHERE end_date BETWEEN DATE '2025-04-17' AND DATE '2026-09-19'
),
enriched_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.product_group,
        p.risk_score AS holder_risk_score,
        p.joined_on AS holder_joined_on,
        p.segment AS holder_segment
    FROM prepared s
    LEFT JOIN policyholders p ON p.id = s.holder_id AND p.risk_score <= 0.8
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(premium - AVG(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', holder_joined_on)), 2) AS premium_vs_holder_joined_on_month_avg,
        MAX(holder_risk_score) OVER (ORDER BY holder_joined_on DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS holder_risk_score_max_2_prior,
        ROUND(AVG(premium) OVER (ORDER BY holder_joined_on DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS premium_avg_4,
        ROW_NUMBER() OVER w_all_by_holder_joined_on AS rn,
        ROUND(holder_risk_score - AVG(holder_risk_score) OVER w_holder_joined_on_month, 2) AS holder_risk_score_vs_holder_joined_on_month_avg
    FROM enriched_holder
    WINDOW w_all_by_holder_joined_on AS (ORDER BY holder_joined_on ASC, id), w_holder_joined_on_month AS (PARTITION BY DATE_TRUNC('MONTH', holder_joined_on))
),
enriched AS (
    -- Derive helper columns
    SELECT *, SUBSTRING(TRIM(holder_segment), 1, 3) AS holder_segment_prefix
    FROM ranked
    WHERE holder_segment = 'VIP'
),
joined_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.holder_risk_score,
        s.holder_risk_score_max_2_prior,
        s.premium_avg_4,
        s.rn,
        s.holder_segment_prefix,
        p.holder_name
    FROM enriched s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
without_matches AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM joined_holder s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.premium < 3465.4
              AND YEAR(p.start_date) = 2025
        )
)
SELECT
    id,
    holder_id,
    premium,
    start_date,
    holder_risk_score,
    holder_risk_score_max_2_prior,
    premium_avg_4,
    rn,
    holder_segment_prefix,
    holder_name,
    CONCAT_WS('|', holder_name, holder_segment_prefix) AS holder_name_holder_segment_prefix_label,
    LOWER(REGEXP_REPLACE(TRIM(holder_name), ' ', '_')) AS holder_name_slug
FROM without_matches
ORDER BY holder_risk_score_max_2_prior DESC, id
LIMIT 5
