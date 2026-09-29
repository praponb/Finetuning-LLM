WITH all_events AS (
    -- Stack the branches
    SELECT
        policy_id AS key_id,
        id AS entity_id,
        claimed_amount,
        'claim' AS origin
    FROM claims
    UNION ALL
    SELECT
        holder_id AS key_id,
        id AS entity_id,
        premium AS claimed_amount,
        'policy' AS origin
    FROM policies
    WHERE status NOT IN ('EXPIRED', 'ACTIVE')
    UNION ALL
    SELECT
        adjuster_id AS key_id,
        policy_id AS entity_id,
        claimed_amount,
        'claim_3' AS origin
    FROM claims
)
SELECT origin, COUNT(*) AS n_rows, SUM(claimed_amount) AS total_claimed_amount
FROM all_events
GROUP BY origin
ORDER BY origin
