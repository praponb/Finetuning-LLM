WITH cleaned_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        reported_at,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket,
        GREATEST(approved_amount, claimed_amount) AS greatest_approved_amount_claimed_amount
    FROM claims
    WHERE claim_type = 'flood'
),
with_details_adjuster AS (
    -- Join adjusters
    SELECT s.*, a.level AS adjuster_level
    FROM cleaned_claims s
    JOIN adjusters a ON a.id = s.adjuster_id
    WHERE a.level >= 2
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_details_adjuster s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.approved_amount > 0.0
        )
)
SELECT id, metric, reading
FROM (
    SELECT id, claimed_amount_bucket, adjuster_level
    FROM screened
) src
UNPIVOT (
    reading FOR metric IN (claimed_amount_bucket AS m_claimed_amount_bucket, adjuster_level AS m_adjuster_level)
) unp
ORDER BY id, metric
