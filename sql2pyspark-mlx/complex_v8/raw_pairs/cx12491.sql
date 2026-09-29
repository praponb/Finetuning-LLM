WITH cleaned AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        CASE WHEN status IN ('OPEN', 'APPROVED') THEN 'group_a' WHEN status = 'PAID' THEN 'group_b' ELSE 'other' END AS status_group
    FROM claims
    WHERE claim_type IN ('collision', 'theft')
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.status_group,
        c.id AS claim_id,
        c.claim_type,
        c.adjuster_id AS claim_adjuster_id
    FROM cleaned s
    LEFT JOIN claims c ON c.policy_id = s.policy_id AND c.claim_type NOT IN ('delay', 'medical')
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        MAX(claim_date) AS last_claim_date,
        MAX(claim_date) AS last_claim_date_2,
        COUNT(CASE WHEN claim_type = 'collision' THEN 1 END) AS claim_claim_type_count
    FROM claims
    GROUP BY adjuster_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.last_claim_date,
        c.last_claim_date_2,
        COALESCE(c.claim_claim_type_count, 0) AS claim_claim_type_count
    FROM detail_rows s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
)
SELECT *
FROM (
    SELECT claim_id, claim_type, claimed_amount
    FROM summarized_rows
) src
PIVOT (
    AVG(claimed_amount)
    FOR claim_type IN ('collision', 'fire')
)
