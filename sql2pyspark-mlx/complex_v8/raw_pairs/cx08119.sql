WITH prepared_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE reported_at >= TIMESTAMP '2025-01-31 12:00:00'
),
combined_details_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.status,
        s.reported_at,
        p.premium AS policy_premium,
        p.status AS policy_status
    FROM prepared_claims s
    LEFT JOIN policies p ON p.id = s.policy_id
),
filtered AS (
    -- Keep rows whose policy matches
    SELECT s.*
    FROM combined_details_policy s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.id = s.policy_id
              AND p.status IN ('ACTIVE', 'CANCELLED', 'EXPIRED')
              AND YEAR(p.end_date) = 2026
        )
),
unique_rows AS (
    -- Keep the latest row per status
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        status,
        reported_at,
        policy_premium,
        policy_status
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status ORDER BY approved_amount DESC, id DESC) AS recency_rank
        FROM filtered
    ) ranked
    WHERE recency_rank = 1
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT adjuster_id, ROUND(AVG(claimed_amount), 2) AS avg_claim_claimed_amount
    FROM claims
    GROUP BY adjuster_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.avg_claim_claimed_amount
    FROM unique_rows s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
)
SELECT
    id,
    policy_id,
    adjuster_id,
    approved_amount,
    reported_at,
    policy_premium,
    policy_status,
    avg_claim_claimed_amount,
    ROUND(avg_claim_claimed_amount / NULLIF(approved_amount, 0), 3) AS avg_claim_claimed_amount_per_approved_amount,
    SUBSTRING(TRIM(status), 1, 3) AS status_prefix
FROM with_counts
ORDER BY policy_id DESC
