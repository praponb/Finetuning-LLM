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
    WHERE claim_type = 'medical'
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        ABS(approved_amount - 15477.7) AS approved_amount_distance,
        ROUND(approved_amount / NULLIF(claimed_amount, 0), 3) AS approved_amount_per_claimed_amount,
        SUBSTRING(TRIM(claim_type), 1, 2) AS claim_type_prefix
    FROM cleaned_claims
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.approved_amount BETWEEN 2349.3 AND 20843.1
              AND c.claim_date >= DATE '2024-06-07'
        )
      AND s.approved_amount_per_claimed_amount >= 3.5
)
SELECT id, metric, value
FROM (
    SELECT id, approved_amount_distance, approved_amount
    FROM filtered
) src
UNPIVOT (
    value FOR metric IN (approved_amount_distance, approved_amount)
) unp
