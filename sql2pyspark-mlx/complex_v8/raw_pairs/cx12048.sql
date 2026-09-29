WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE status IS NOT NULL
      AND approved_amount BETWEEN 0.0 AND 38386.6
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(4) OVER (ORDER BY reported_at DESC NULLS FIRST, id) AS reported_at_quartile,
        MAX(approved_amount) OVER (PARTITION BY policy_id) AS policy_id_max_approved_amount,
        LAG(claim_date, 3) OVER w_all_by_reported_at AS prev3_claim_date,
        MIN(approved_amount) OVER (PARTITION BY policy_id) AS policy_id_min_approved_amount
    FROM filtered
    WINDOW w_all_by_reported_at AS (ORDER BY reported_at DESC NULLS FIRST, id)
),
latest AS (
    -- Keep the latest row per adjuster_id, status
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
        reported_at_quartile,
        policy_id_max_approved_amount,
        prev3_claim_date,
        policy_id_min_approved_amount
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id, status ORDER BY claim_date DESC, id DESC) AS recency_rank
        FROM ranked
    ) ranked
    WHERE recency_rank = 1
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.approved_amount,
        s.reported_at,
        s.policy_id_max_approved_amount,
        s.prev3_claim_date,
        c.id AS claim_id,
        c.claim_type AS claim_claim_type,
        c.claimed_amount AS claim_claimed_amount
    FROM latest s
    LEFT JOIN claims c ON c.policy_id = s.policy_id
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claimed_amount,
    approved_amount,
    reported_at,
    prev3_claim_date,
    claim_id,
    claimed_amount - approved_amount AS claimed_amount_minus_approved_amount,
    CASE WHEN approved_amount >= 14595.6 THEN 'A' WHEN approved_amount >= 2569.2 THEN 'B' ELSE 'C' END AS approved_amount_tier
FROM line_level_claims
WHERE claimed_amount > 13635.1
ORDER BY claimed_amount_minus_approved_amount, claim_id DESC, id
LIMIT 10
