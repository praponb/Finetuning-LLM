WITH cleaned AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        reported_at + INTERVAL 7 DAYS AS reported_at_plus_7d
    FROM claims
    WHERE reported_at >= TIMESTAMP '2025-01-31 13:00:00'
),
status_stats AS (
    -- Avg approved_amount per status
    SELECT status, AVG(approved_amount) AS avg_approved_amount
    FROM cleaned
    GROUP BY status
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_approved_amount,
        ROUND(s.approved_amount - g.avg_approved_amount, 2) AS approved_amount_minus_avg
    FROM cleaned s
    JOIN status_stats g ON g.status = s.status
    WHERE s.approved_amount > g.avg_approved_amount
),
with_parent_policy AS (
    -- Join policies
    SELECT
        s.*,
        p.end_date AS policy_end_date,
        p.holder_id AS policy_holder_id,
        p.status AS policy_status
    FROM outliers s
    JOIN policies p ON p.id = s.policy_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY adjuster_id, claim_type ORDER BY reported_at DESC, id) AS reported_at_decile,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY status, policy_holder_id)) AS distinct_adjuster_id_in_status_policy_holder_id,
        DATEDIFF(claim_date, LAG(claim_date) OVER (ORDER BY claim_date ASC, id)) AS days_since_prev_claim_date,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY status, policy_holder_id), 2) AS status_policy_holder_id_avg_claimed_amount
    FROM with_parent_policy
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        reported_at_plus_7d,
        avg_approved_amount,
        approved_amount_minus_avg,
        policy_end_date,
        policy_holder_id,
        distinct_adjuster_id_in_status_policy_holder_id,
        days_since_prev_claim_date,
        COALESCE(policy_status, 'UNKNOWN') AS policy_status_or_unknown
    FROM metrics
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    approved_amount,
    reported_at_plus_7d,
    policy_end_date,
    policy_holder_id,
    distinct_adjuster_id_in_status_policy_holder_id,
    policy_status_or_unknown
FROM annotated
WHERE approved_amount >= 0.0
