WITH prepared_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status
    FROM claims
    WHERE claimed_amount >= 4868.7
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY claim_type, adjuster_id), 0), 4) AS claimed_amount_share_of_claim_type_adjuster_id,
        SIZE(COLLECT_SET(policy_id) OVER w_claim_type_adjuster_id) AS distinct_policy_id_in_claim_type_adjuster_id
    FROM prepared_claims
    WINDOW w_claim_type_adjuster_id AS (PARTITION BY claim_type, adjuster_id)
)
SELECT id, measure, reading
FROM (
    SELECT id, claimed_amount_share_of_claim_type_adjuster_id, claimed_amount
    FROM sequenced
) src
UNPIVOT INCLUDE NULLS (
    reading FOR measure IN (claimed_amount_share_of_claim_type_adjuster_id AS claimed_amount_share_of_claim_type_adjuster_id_v, claimed_amount AS m_claimed_amount)
) unp
