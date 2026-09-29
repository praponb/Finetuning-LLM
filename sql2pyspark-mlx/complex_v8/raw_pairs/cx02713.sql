WITH cleaned AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
    WHERE team <> 'Travel'
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.adjuster_name,
        s.team,
        s.level,
        c.id AS claim_id,
        c.policy_id AS claim_policy_id,
        c.approved_amount AS claim_approved_amount,
        c.claimed_amount AS claim_claimed_amount,
        c.claim_type
    FROM cleaned s
    LEFT JOIN claims c ON c.adjuster_id = s.id
),
deduped AS (
    -- Keep the latest row per claim_policy_id
    SELECT
        id,
        adjuster_name,
        team,
        level,
        claim_id,
        claim_policy_id,
        claim_approved_amount,
        claim_claimed_amount,
        claim_type
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_policy_id ORDER BY claim_approved_amount DESC, id DESC, claim_id DESC) AS recency_rank
        FROM detail_rows
    ) ranked
    WHERE recency_rank = 1
)
SELECT id, claim_id, metric, amount
FROM (
    SELECT id, claim_id, claim_claimed_amount, claim_approved_amount
    FROM deduped
) src
UNPIVOT (
    amount FOR metric IN (claim_claimed_amount, claim_approved_amount)
) unp
