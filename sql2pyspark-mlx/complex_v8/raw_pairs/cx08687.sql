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
        reported_at
    FROM claims
    WHERE claimed_amount <= 29359.4
      AND reported_at < TIMESTAMP '2025-02-18 09:00:00'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER w_status AS rows_in_status,
        ROW_NUMBER() OVER w_all_by_reported_at AS reported_at_seq,
        MAX(claimed_amount) OVER (ORDER BY reported_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS claimed_amount_max_4,
        SUM(claimed_amount) OVER w_status AS status_sum_claimed_amount,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY status)) AS distinct_claim_type_in_status
    FROM cleaned_claims
    WINDOW w_status AS (PARTITION BY status), w_all_by_reported_at AS (ORDER BY reported_at, id)
),
with_details AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.reported_at,
        s.reported_at_seq,
        s.claimed_amount_max_4,
        s.status_sum_claimed_amount,
        p.status AS policy_status,
        p.premium AS policy_premium,
        p.product AS policy_product,
        po.region AS holder_region,
        po.joined_on AS holder_joined_on
    FROM ranked s
    JOIN policies p ON p.id = s.policy_id
    JOIN policyholders po ON po.id = p.holder_id
)
SELECT *
FROM (
    SELECT policy_id, reported_at, policy_status, status_sum_claimed_amount
    FROM with_details
) src
PIVOT (
    AVG(status_sum_claimed_amount)
    FOR policy_status IN ('CANCELLED', 'EXPIRED', 'ACTIVE', 'LAPSED')
)
ORDER BY policy_id, reported_at
