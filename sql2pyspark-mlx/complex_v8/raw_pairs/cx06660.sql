WITH valid_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE claim_date < DATE '2025-04-20'
      AND status IN ('REJECTED', 'PAID')
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.approved_amount,
        c.id AS claim_id,
        c.claim_type AS claim_claim_type,
        c.reported_at AS claim_reported_at
    FROM valid_claims s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(c.approved_amount), 2) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS avg_claim_approved_amount
    FROM exploded_detail s
),
enriched_policy AS (
    -- Join policies
    SELECT
        s.*,
        p.end_date AS policy_end_date,
        p.premium AS policy_premium,
        p.product AS policy_product
    FROM with_counts s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.premium >= 1541.8
),
with_activity AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM enriched_policy s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.approved_amount > 7204.1
              AND c.status IN ('OPEN', 'PAID', 'IN_REVIEW')
        )
      AND s.policy_product <> 'TRAVEL'
),
claim_stats_2 AS (
    -- Per-policy claims metrics
    SELECT policy_id, COUNT(*) AS claim_count
    FROM claims
    GROUP BY policy_id
),
with_counts_2 AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count
    FROM with_activity s
    LEFT JOIN claim_stats_2 c ON c.policy_id = s.policy_id
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        claim_id,
        claim_reported_at,
        avg_claim_approved_amount,
        policy_premium,
        claim_count,
        DATEDIFF(DATE '2025-12-31', policy_end_date) AS days_since_policy_end_date,
        ABS(policy_premium - 2721.8) AS policy_premium_distance
    FROM with_counts_2
    WHERE claim_id >= 44
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT adjuster_id, claim_reported_at, claim_type, claim_count
    FROM annotated
) src
PIVOT (
    MAX(claim_count)
    FOR claim_type IN ('theft', 'medical')
)
ORDER BY adjuster_id, claim_reported_at
