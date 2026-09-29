WITH filtered_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket,
        LENGTH(TRIM(status)) AS status_len,
        GREATEST(claimed_amount, approved_amount) AS greatest_claimed_amount_approved_amount
    FROM claims
    WHERE adjuster_id IS NOT NULL
      AND approved_amount < 19337.6
),
combined_details_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.claimed_amount_bucket,
        s.greatest_claimed_amount_approved_amount,
        a.adjuster_name,
        a.team AS adjuster_team,
        a.level AS adjuster_level
    FROM filtered_claims s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, amount
FROM (
    SELECT id, adjuster_level, claimed_amount_bucket
    FROM combined_details_adjuster
) src
UNPIVOT (
    amount FOR field IN (adjuster_level AS adjuster_level_v, claimed_amount_bucket AS claimed_amount_bucket_v)
) unp
ORDER BY id, field
