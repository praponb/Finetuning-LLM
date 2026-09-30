WITH filtered_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        CASE WHEN product IN ('AUTO', 'LIFE') THEN 'group_a' WHEN product = 'TRAVEL' THEN 'group_b' ELSE 'other' END AS product_group,
        DATE_ADD(end_date, 30) AS end_date_plus_30d,
        DATE_TRUNC('WEEK', end_date) AS end_date_week
    FROM policies
),
without_matches AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM filtered_policys s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.status IS NOT NULL
        )
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
        s.product_group,
        s.end_date_plus_30d,
        s.end_date_week,
        p.risk_score AS holder_risk_score,
        p.holder_name
    FROM without_matches s
    JOIN policyholders p ON p.id = s.holder_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER (PARTITION BY product, MONTH(end_date_plus_30d)), 0), 4) AS holder_risk_score_share_of_product_end_date_plus,
        ROW_NUMBER() OVER w_end_date_month_by_end_date_p AS seq_no,
        ROUND(CUME_DIST() OVER w_all_by_premium, 3) AS premium_cume_dist
    FROM with_details
    WINDOW w_end_date_month_by_end_date_p AS (PARTITION BY MONTH(end_date) ORDER BY end_date_plus_30d DESC, id), w_all_by_premium AS (ORDER BY premium, id)
)
-- Subtotals with GROUPING SETS
SELECT
    product_group,
    holder_id,
    GROUPING_ID() AS grouping_level,
    ROUND(AVG(holder_risk_score_share_of_product_end_date_plus), 2) AS avg_holder_risk_score_share_of_product_end_date_,
    COUNT(DISTINCT product_group) AS distinct_product_groups,
    COUNT(*) AS row_count
FROM with_trends
GROUP BY GROUPING SETS ((product_group, holder_id), (product_group), ())
