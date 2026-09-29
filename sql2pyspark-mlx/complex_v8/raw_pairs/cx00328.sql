WITH recent_claims AS (
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
        COALESCE(approved_amount, 0.0) AS approved_amount_filled,
        LENGTH(TRIM(status)) AS status_len
    FROM claims
    WHERE (MONTH(reported_at) IN (2, 3) OR claimed_amount <= 31752.9)
),
tagged AS (
    -- Add calculated columns
    SELECT *, LOWER(TRIM(status)) AS status_clean
    FROM recent_claims
),
combined_details_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.approved_amount_filled,
        s.status_len,
        s.status_clean,
        a.adjuster_name,
        p.product AS policy_product
    FROM tagged s
    JOIN adjusters a ON a.id = s.adjuster_id AND a.team = 'Health'
    LEFT JOIN policies p ON p.id = s.policy_id
    WHERE LENGTH(TRIM(a.adjuster_name)) >= 4
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM combined_details_adjuster s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.reported_at < TIMESTAMP '2025-03-02 14:00:00'
              AND c.claimed_amount >= 18924.4
        )
)
SELECT *
FROM (
    SELECT policy_id, claim_date, claim_type, approved_amount_filled
    FROM qualified
) src
PIVOT (
    MAX(approved_amount_filled)
    FOR claim_type IN ('fire', 'medical', 'theft')
)
