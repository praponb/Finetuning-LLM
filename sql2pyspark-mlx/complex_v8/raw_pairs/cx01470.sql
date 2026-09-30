WITH base AS (
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
        ADD_MONTHS(claim_date, 6) AS claim_date_plus_6m
    FROM claims
    WHERE policy_id IS NOT NULL
      AND claim_type IN ('collision', 'flood', 'medical')
)
SELECT
    'claim' AS src,
    adjuster_id,
    policy_id,
    reported_at AS happened_at
FROM base
UNION ALL
SELECT
    'claim_2' AS src,
    id AS adjuster_id,
    adjuster_id AS policy_id,
    reported_at AS happened_at
FROM claims
UNION ALL
SELECT
    'claim_3' AS src,
    adjuster_id,
    id AS policy_id,
    reported_at AS happened_at
FROM claims
