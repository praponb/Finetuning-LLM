WITH recent_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown
    FROM claims
    WHERE reported_at < TIMESTAMP '2025-02-18 11:00:00'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        claimed_amount,
        status,
        claim_type_or_unknown,
        ROUND(AVG(approved_amount) OVER (PARTITION BY claim_type_or_unknown, adjuster_id), 2) AS claim_type_or_unknown_adjuster_id_avg_approved_a,
        RANK() OVER w_all_by_claim_date_ties AS claim_date_rank,
        MAX(claimed_amount) OVER w_claim_type_or_unknown_adjust AS claim_type_or_unknown_adjuster_id_max_claimed_am
    FROM recent_claims
    WINDOW w_all_by_claim_date_ties AS (ORDER BY claim_date DESC), w_claim_type_or_unknown_adjust AS (PARTITION BY claim_type_or_unknown, adjuster_id)
),
enriched_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.status,
        s.claim_type_or_unknown,
        s.claim_type_or_unknown_adjuster_id_max_claimed_am,
        p.status AS policy_status
    FROM ranked s
    LEFT JOIN policies p ON p.id = s.policy_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        claim_type_or_unknown_adjuster_id_max_claimed_am,
        policy_status,
        NTILE(10) OVER (PARTITION BY status, claim_type_or_unknown ORDER BY claim_type_or_unknown_adjuster_id_max_claimed_am DESC, id) AS claim_type_or_unknown_adjuster_id_max_claimed_am_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status, claim_type_or_unknown ORDER BY claim_type_or_unknown_adjuster_id_max_claimed_am DESC, id), 3) AS claim_type_or_unknown_adjuster_id_max_claimed_am_3,
        RANK() OVER (PARTITION BY status, claim_type_or_unknown ORDER BY claim_type_or_unknown_adjuster_id_max_claimed_am DESC, id) AS claim_type_or_unknown_adjuster_id_max_claimed_am_4
    FROM enriched_policy
)
SELECT *
FROM (
    SELECT claim_type_or_unknown_adjuster_id_max_claimed_am_2, claim_type_or_unknown_adjuster_id_max_claimed_am_4, policy_status, claim_type_or_unknown_adjuster_id_max_claimed_am
    FROM sequenced
) src
PIVOT (
    SUM(claim_type_or_unknown_adjuster_id_max_claimed_am)
    FOR policy_status IN ('CANCELLED', 'ACTIVE')
)
