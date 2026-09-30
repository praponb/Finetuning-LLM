WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        CASE WHEN premium >= 3465.4 THEN 'HIGH' WHEN premium >= 1541.8 THEN 'MEDIUM' ELSE 'LOW' END AS premium_tier,
        DATE_TRUNC('MONTH', start_date) AS start_date_month
    FROM policies
    WHERE (status = 'CANCELLED' OR YEAR(start_date) = 2025)
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        ROUND(AVG(premium), 2) AS avg_policy_premium,
        COUNT(*) AS policy_count,
        SUM(premium) AS total_policy_premium
    FROM policies
    GROUP BY holder_id
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        p.avg_policy_premium,
        COALESCE(p.policy_count, 0) AS policy_count,
        p.total_policy_premium
    FROM prepared s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
),
combined_details_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.status,
        s.premium_tier,
        s.avg_policy_premium,
        s.policy_count,
        s.total_policy_premium,
        p.joined_on AS holder_joined_on,
        p.segment AS holder_segment,
        p.risk_score AS holder_risk_score
    FROM summarized_rows s
    JOIN policyholders p ON p.id = s.holder_id AND LOWER(p.holder_name) LIKE '%rt%'
)
SELECT
    status,
    holder_segment,
    GROUPING(status) AS is_total_status,
    ROUND(100 * COUNT(CASE WHEN holder_segment = 'SME' THEN 1 END) / COUNT(*), 1) AS pct_holder_segment_sme,
    MAX(start_date) AS last_start_date,
    ARRAY_SORT(COLLECT_SET(holder_segment)) AS holder_segment_list
FROM combined_details_holder
GROUP BY GROUPING SETS ((status, holder_segment), (status), ())
ORDER BY status NULLS LAST, holder_segment NULLS LAST
