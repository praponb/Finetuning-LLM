WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        CASE WHEN product IN ('LIFE', 'AUTO') THEN 'group_a' WHEN product = 'TRAVEL' THEN 'group_b' ELSE 'other' END AS product_group,
        DATEDIFF(DATE '2026-01-01', end_date) AS days_since_end_date
    FROM policies
    WHERE holder_id >= 9
      AND status = 'LAPSED'
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        product_group,
        days_since_end_date,
        ROUND(days_since_end_date / NULLIF(premium, 0), 3) AS days_since_end_date_per_premium,
        CASE WHEN product_group IN ('group_a', 'group_b') THEN 'group_a' WHEN product_group = 'other' THEN 'group_b' ELSE 'other' END AS product_group_group,
        COALESCE(premium, 0.0) AS premium_filled
    FROM scoped_policys
),
stats AS (
    -- Group-level totals
    SELECT
        product_group,
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        holder_id,
        ROUND(STDDEV(days_since_end_date_per_premium), 2) AS stddev_days_since_end_date_per_premium,
        MAX(end_date) AS last_end_date
    FROM labeled
    WHERE product_group_group IN ('other', 'group_b')
    GROUP BY 1, 2, 3
    HAVING MAX(days_since_end_date_per_premium) > 2.0
),
joined_holder AS (
    -- Enrich with dimension data
    SELECT
        s.product_group,
        s.end_date_month,
        s.holder_id,
        s.stddev_days_since_end_date_per_premium,
        s.last_end_date,
        p.joined_on AS holder_joined_on,
        p.risk_score AS holder_risk_score
    FROM stats s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.joined_on >= DATE '2024-06-18'
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        product_group,
        end_date_month,
        holder_id,
        stddev_days_since_end_date_per_premium,
        last_end_date,
        holder_joined_on,
        holder_risk_score,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY product_group)) AS distinct_holder_id_in_product_group,
        ROUND(AVG(holder_risk_score) OVER (PARTITION BY product_group), 2) AS product_group_avg_holder_risk_score
    FROM joined_holder
),
qualified AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM with_trends s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.product = 'HOME'
              AND p.status = 'CANCELLED'
        )
      AND s.holder_id <= 15
)
SELECT
    product_group,
    holder_id,
    stddev_days_since_end_date_per_premium,
    last_end_date,
    holder_risk_score,
    distinct_holder_id_in_product_group,
    product_group_avg_holder_risk_score
FROM qualified
ORDER BY holder_id DESC NULLS FIRST, product_group_avg_holder_risk_score
