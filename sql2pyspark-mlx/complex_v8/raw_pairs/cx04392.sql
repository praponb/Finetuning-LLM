WITH recent_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        reported_at,
        TO_DATE(reported_at) AS reported_at_date,
        claimed_amount + approved_amount AS claimed_amount_plus_approved_amount
    FROM claims
    WHERE status IN ('PAID', 'REJECTED')
      AND reported_at < TIMESTAMP '2025-03-02 12:00:00'
),
without_matches AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM recent_claims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND MONTH(c.reported_at) IN (1, 3)
        )
)
SELECT
    adjuster_id,
    policy_id,
    COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids,
    ROUND(AVG(approved_amount), 2) AS avg_approved_amount,
    MIN(approved_amount) AS min_approved_amount
FROM without_matches
GROUP BY ROLLUP(adjuster_id, policy_id)
ORDER BY adjuster_id NULLS LAST, policy_id NULLS LAST
