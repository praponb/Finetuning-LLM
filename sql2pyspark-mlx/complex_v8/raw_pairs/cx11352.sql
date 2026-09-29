WITH filtered_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        reported_at,
        DATE_FORMAT(reported_at, 'yyyy-MM-dd HH:00') AS reported_at_label,
        CONCAT_WS('|', status, claim_type) AS status_claim_type_label,
        LAST_DAY(claim_date) AS claim_date_month_end
    FROM claims
    WHERE (approved_amount >= 5336.7 OR claim_type IS NOT NULL)
),
with_details_policy AS (
    -- Enrich with dimension data
    SELECT s.*, p.premium AS policy_premium
    FROM filtered_claims s
    LEFT JOIN policies p ON p.id = s.policy_id
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        policy_id,
        DATE_TRUNC('MONTH', claim_date_month_end) AS claim_date_month_end_month,
        ROUND(SUM(approved_amount), 2) AS total_approved_amount,
        COUNT(*) AS row_count,
        ROUND(STDDEV(policy_premium), 2) AS stddev_policy_premium
    FROM with_details_policy
    GROUP BY DATE_TRUNC('MONTH', claim_date), policy_id, DATE_TRUNC('MONTH', claim_date_month_end)
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_group
    WHERE total_approved_amount > (SELECT AVG(total_approved_amount) FROM per_group)
)
-- Final projection
SELECT
    claim_date_month,
    policy_id,
    claim_date_month_end_month,
    total_approved_amount,
    stddev_policy_premium,
    claim_date_month + INTERVAL 2 HOURS AS claim_date_month_plus_2h,
    ROUND(total_approved_amount / NULLIF(row_count, 0), 3) AS total_approved_amount_per_row_count
FROM screened
ORDER BY total_approved_amount DESC, claim_date_month, policy_id, claim_date_month_end_month
LIMIT 5
