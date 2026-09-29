WITH prepared_claims AS (
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
        reported_at + INTERVAL 30 MINUTES AS reported_at_plus_30m,
        SUBSTRING(TRIM(claim_type), 1, 3) AS claim_type_prefix
    FROM claims
    WHERE MONTH(reported_at) IN (1, 3)
      AND approved_amount < 28426.9
),
with_activity AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM prepared_claims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.claimed_amount > 10518.4
              AND c.claim_date < DATE '2025-05-26'
        )
),
enriched_adjuster AS (
    -- Enrich with dimension data
    SELECT s.*, a.level AS adjuster_level, p.start_date AS policy_start_date
    FROM with_activity s
    JOIN adjusters a ON a.id = s.adjuster_id
    LEFT JOIN policies p ON p.id = s.policy_id
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        claim_type_prefix,
        adjuster_level,
        policy_start_date,
        approved_amount + claimed_amount AS approved_amount_plus_claimed_amount,
        LENGTH(TRIM(claim_type_prefix)) AS claim_type_prefix_len
    FROM enriched_adjuster
    WHERE claimed_amount < 41878.5
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(adjuster_level - 3) AS adjuster_level_distance,
        TO_DATE(reported_at) AS reported_at_date,
        DATEDIFF(claim_date, policy_start_date) AS days_policy_start_date_to_claim_date
    FROM annotated
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claim_type,
    claimed_amount,
    reported_at,
    adjuster_level,
    policy_start_date,
    approved_amount_plus_claimed_amount,
    adjuster_level_distance,
    reported_at_date,
    days_policy_start_date_to_claim_date,
    DATEDIFF(DATE '2026-01-01', reported_at_date) AS days_since_reported_at_date,
    LOWER(TRIM(claim_type)) AS claim_type_clean
FROM derived
ORDER BY claimed_amount, id
LIMIT 10
