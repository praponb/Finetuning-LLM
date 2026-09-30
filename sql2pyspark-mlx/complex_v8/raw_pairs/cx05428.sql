WITH prepared AS (
    -- Keep valid rows and derive helper columns
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
        DATEDIFF(DATE '2026-01-01', reported_at) AS days_since_reported_at,
        ABS(claimed_amount - 20436.5) AS claimed_amount_distance,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled
    FROM claims
    WHERE approved_amount <= 28426.9
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM prepared s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
        )
      AND s.claimed_amount BETWEEN 10518.4 AND 33975.8
),
deduped AS (
    -- Keep the earliest row per policy_id
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
        days_since_reported_at,
        claimed_amount_distance,
        approved_amount_filled
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY claimed_amount_distance, id) AS rn
        FROM screened
    ) ranked
    WHERE rn = 1
),
joined_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.claimed_amount_distance,
        s.approved_amount_filled,
        p.holder_id AS policy_holder_id,
        p.premium AS policy_premium
    FROM deduped s
    LEFT JOIN policies p ON p.id = s.policy_id
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        approved_amount_filled,
        policy_holder_id,
        LEAST(approved_amount_filled, claimed_amount) AS least_approved_amount_filled_claimed_amount
    FROM joined_policy
    WHERE claim_date < DATE '2025-09-19'
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT policy_holder_id, claim_type, status, approved_amount_filled
    FROM enriched
) src
PIVOT (
    SUM(approved_amount_filled)
    FOR status IN ('OPEN', 'REJECTED', 'IN_REVIEW')
)
ORDER BY policy_holder_id, claim_type
