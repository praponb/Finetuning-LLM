WITH base AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status,
        claimed_amount + approved_amount AS claimed_amount_plus_approved_amount,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled,
        CONCAT(status, '#', CAST(policy_id AS STRING)) AS status_ref
    FROM claims
    WHERE status = 'PAID'
),
status_stats AS (
    -- Max approved_amount per status
    SELECT status, MAX(approved_amount) AS max_approved_amount
    FROM base
    GROUP BY status
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_approved_amount,
        ROUND(s.approved_amount - g.max_approved_amount, 2) AS approved_amount_minus_max
    FROM base s
    JOIN status_stats g ON g.status = s.status
    WHERE s.approved_amount = g.max_approved_amount
),
adjuster_id_stats AS (
    -- Median max_approved_amount per adjuster_id
    SELECT adjuster_id, PERCENTILE_APPROX(max_approved_amount, 0.5) AS median_max_approved_amount
    FROM outliers
    GROUP BY adjuster_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_max_approved_amount,
        ROUND(s.max_approved_amount - g.median_max_approved_amount, 2) AS max_approved_amount_minus_median
    FROM outliers s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.max_approved_amount >= g.median_max_approved_amount
),
stats AS (
    -- Summarise per group
    SELECT
        status,
        policy_id,
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        SUM(approved_amount_minus_max) AS total_approved_amount_minus_max,
        MAX(claim_date) AS last_claim_date,
        ROUND(AVG(approved_amount_filled), 2) AS avg_approved_amount_filled,
        ROUND(SUM(max_approved_amount_minus_median) / NULLIF(SUM(approved_amount_minus_max), 0), 3) AS max_approved_amount_minus_median_to_approved_amo,
        MAX(max_approved_amount_minus_median) AS max_max_approved_amount_minus_median
    FROM compared
    GROUP BY 1, 2, 3
),
with_details AS (
    -- Join policies
    SELECT
        s.status,
        s.policy_id,
        s.claim_date_month,
        s.total_approved_amount_minus_max,
        s.last_claim_date,
        s.avg_approved_amount_filled,
        s.max_approved_amount_minus_median_to_approved_amo,
        s.max_max_approved_amount_minus_median,
        p.start_date AS policy_start_date,
        p.premium AS policy_premium,
        po.holder_name
    FROM stats s
    JOIN policies p ON p.id = s.policy_id
    JOIN policyholders po ON po.id = p.holder_id
),
combined_details_policy AS (
    -- Enrich with dimension data
    SELECT
        s.status,
        s.policy_id,
        s.claim_date_month,
        s.last_claim_date,
        s.max_approved_amount_minus_median_to_approved_amo,
        s.max_max_approved_amount_minus_median,
        s.policy_premium,
        p.holder_id AS policy_holder_id,
        p.end_date AS policy_end_date,
        po.holder_name AS holder_holder_name,
        po.segment AS holder_segment
    FROM with_details s
    JOIN policies p ON p.id = s.policy_id
    JOIN policyholders po ON po.id = p.holder_id
)
-- Final projection
SELECT
    status,
    policy_id,
    claim_date_month,
    last_claim_date,
    max_approved_amount_minus_median_to_approved_amo,
    policy_premium,
    policy_holder_id,
    policy_end_date,
    holder_holder_name,
    holder_segment,
    ROUND(policy_premium * 1.5, 2) AS policy_premium_adj
FROM combined_details_policy
