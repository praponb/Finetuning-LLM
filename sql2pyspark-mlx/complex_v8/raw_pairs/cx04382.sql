WITH filtered_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        reported_at,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        CONCAT_WS('|', status, claim_type) AS status_claim_type_label
    FROM claims
    WHERE policy_id <= 25
      AND adjuster_id <= 2
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        MAX(claimed_amount) AS max_claim_claimed_amount,
        MAX(claim_date) AS last_claim_date
    FROM claims
    GROUP BY policy_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.max_claim_claimed_amount,
        c.last_claim_date
    FROM filtered_claims s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_counts s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.claimed_amount < 38909.4
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, amount
FROM (
    SELECT id, max_claim_claimed_amount, claimed_amount
    FROM filtered
) src
UNPIVOT (
    amount FOR field IN (max_claim_claimed_amount AS m_max_claim_claimed_amount, claimed_amount AS claimed_amount_v)
) unp
