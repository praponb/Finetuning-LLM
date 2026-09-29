WITH filtered AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        reported_at
    FROM claims
    WHERE (reported_at >= TIMESTAMP '2025-02-12 07:00:00' OR status NOT IN ('APPROVED', 'PAID'))
),
combined AS (
    -- Stack the branches
    SELECT id, adjuster_id, claimed_amount
    FROM filtered
    UNION ALL
    SELECT holder_id, id, premium
    FROM policies
    UNION ALL
    SELECT holder_id, id, premium
    FROM policies
    WHERE start_date < DATE '2025-05-30'
)
SELECT id, COUNT(*) AS n_rows, SUM(claimed_amount) AS total_claimed_amount
FROM combined
GROUP BY id
ORDER BY id
