WITH prepared AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        reported_at
    FROM claims
    WHERE claim_date IS NOT NULL
),
without_matches AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM prepared s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.claim_date >= DATE '2024-04-11'
        )
),
latest_per_key AS (
    -- Keep the latest row per adjuster_id, claim_type
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        reported_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id, claim_type ORDER BY claimed_amount DESC, id DESC) AS row_num
        FROM without_matches
    ) ranked
    WHERE row_num = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT policy_id, adjuster_id, claim_type, claimed_amount
    FROM latest_per_key
) src
PIVOT (
    AVG(claimed_amount)
    FOR claim_type IN ('delay', 'collision', 'medical')
)
