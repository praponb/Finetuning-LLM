WITH filtered_claims AS (
    -- Filter claims
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
        TO_DATE(reported_at) AS reported_at_date,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref
    FROM claims
    WHERE (approved_amount > 2569.2 OR adjuster_id <= 7)
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_TRUNC('QUARTER', claim_date) AS claim_date_quarter,
        approved_amount + claimed_amount AS approved_amount_plus_claimed_amount
    FROM filtered_claims
    WHERE status = 'PAID'
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.claim_date) FROM claims c WHERE c.policy_id = s.policy_id) AS last_claim_date,
        (SELECT COUNT(*) FILTER (WHERE c.status NOT IN ('REJECTED', 'APPROVED')) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_status_count
    FROM enriched s
),
screened AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM with_counts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.claim_type = 'medical'
              AND c.status IS NOT NULL
        )
      AND s.claimed_amount <= 43500.4
),
with_details AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.reported_at_date,
        s.status_ref,
        s.approved_amount_plus_claimed_amount,
        s.last_claim_date,
        s.claim_status_count,
        a.adjuster_name,
        a.team AS adjuster_team,
        a.level AS adjuster_level
    FROM screened s
    JOIN adjusters a ON a.id = s.adjuster_id AND a.team = 'Travel'
)
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
    reported_at_date,
    status_ref,
    approved_amount_plus_claimed_amount,
    adjuster_name,
    adjuster_level,
    ROUND(approved_amount * 1.1, 2) AS approved_amount_adj
FROM with_details
WHERE approved_amount_plus_claimed_amount <= 70000.0
ORDER BY claim_type DESC
