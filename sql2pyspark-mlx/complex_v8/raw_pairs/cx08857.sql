WITH prepared_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status
    FROM claims
),
adjuster_id_stats AS (
    -- Max approved_amount per adjuster_id
    SELECT adjuster_id, MAX(approved_amount) AS max_approved_amount
    FROM prepared_claims
    GROUP BY adjuster_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_approved_amount,
        ROUND(s.approved_amount - g.max_approved_amount, 2) AS approved_amount_minus_max
    FROM prepared_claims s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.approved_amount = g.max_approved_amount
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.approved_amount,
        s.status,
        s.approved_amount_minus_max,
        p.status AS policy_status
    FROM above_average s
    JOIN policies p ON p.id = s.policy_id
)
SELECT
    id,
    policy_id,
    claim_type,
    status,
    approved_amount_minus_max
FROM enriched
ORDER BY approved_amount_minus_max DESC
