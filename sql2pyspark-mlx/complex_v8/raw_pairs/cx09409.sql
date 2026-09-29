WITH scoped AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown
    FROM claims
    WHERE claim_date BETWEEN DATE '2024-02-19' AND DATE '2025-08-10'
      AND status IS NOT NULL
),
with_details_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.approved_amount,
        s.claim_type_or_unknown,
        p.premium AS policy_premium,
        p.start_date AS policy_start_date,
        p.end_date AS policy_end_date
    FROM scoped s
    LEFT JOIN policies p ON p.id = s.policy_id
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(CASE WHEN claim_type IN ('fire', 'collision') THEN 1 END) AS claim_claim_type_count
    FROM claims
    GROUP BY policy_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_claim_type_count, 0) AS claim_claim_type_count
    FROM with_details_policy s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        claim_type_or_unknown,
        policy_end_date,
        claim_claim_type_count,
        DATEDIFF(DATE '2025-06-30', claim_date) AS days_since_claim_date,
        ABS(policy_premium - 2096.6) AS policy_premium_distance
    FROM profiled
    WHERE policy_start_date >= DATE '2024-12-03'
),
joined_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.approved_amount,
        s.policy_end_date,
        s.days_since_claim_date,
        p.start_date AS policy_start_date,
        p.premium AS policy_premium
    FROM scored s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.premium BETWEEN 1541.8 AND 4906.3
),
claim_stats_2 AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(*) FILTER (WHERE status = 'IN_REVIEW') AS claim_status_count,
        COUNT(*) AS claim_count,
        SUM(claimed_amount) AS total_claim_claimed_amount
    FROM claims
    GROUP BY policy_id
),
profiled_2 AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_status_count, 0) AS claim_status_count,
        COALESCE(c.claim_count, 0) AS claim_count,
        c.total_claim_claimed_amount
    FROM joined_policy s
    LEFT JOIN claim_stats_2 c ON c.policy_id = s.policy_id
)
SELECT
    id,
    adjuster_id,
    claim_date,
    approved_amount,
    policy_end_date,
    days_since_claim_date,
    policy_premium,
    claim_status_count,
    claim_count,
    LEAST(days_since_claim_date, claim_status_count) AS least_days_since_claim_date_claim_status_count,
    COALESCE(claim_status_count, 0) AS claim_status_count_filled
FROM profiled_2
ORDER BY adjuster_id, id
LIMIT 10
