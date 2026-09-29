WITH cleaned_claims AS (
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
        SUBSTRING(TRIM(status), 1, 4) AS status_prefix,
        HOUR(reported_at) AS reported_at_hour
    FROM claims
    WHERE adjuster_id >= 3
      AND claimed_amount > 15381.6
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM cleaned_claims s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
        )
),
qualified AS (
    -- Keep rows whose adjuster matches
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM adjusters a
            WHERE a.id = s.adjuster_id
              AND a.team IS NOT NULL
        )
),
enriched AS (
    -- Attach policy attributes
    SELECT s.*, p.premium AS policy_premium
    FROM qualified s
    LEFT JOIN policies p ON p.id = s.policy_id
)
-- Final projection
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
    status_prefix,
    reported_at_hour,
    policy_premium,
    ABS(policy_premium - 3521.8) AS policy_premium_distance,
    TO_DATE(reported_at) AS reported_at_date
FROM enriched
ORDER BY approved_amount DESC, id
LIMIT 5
