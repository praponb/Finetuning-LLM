WITH cleaned_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        status,
        reported_at,
        LEAST(claimed_amount, approved_amount) AS least_claimed_amount_approved_amount,
        DATEDIFF(DATE '2026-01-01', reported_at) AS days_since_reported_at,
        SUBSTRING(TRIM(claim_type), 1, 3) AS claim_type_prefix
    FROM claims
    WHERE claim_type <> 'theft'
),
summary AS (
    -- Group-level totals
    SELECT
        policy_id,
        DATE_FORMAT(reported_at, 'yyyy-MM') AS reported_at_ym,
        PERCENTILE_APPROX(approved_amount, 0.5) AS median_approved_amount,
        MIN(reported_at) AS first_reported_at
    FROM cleaned_claims
    GROUP BY policy_id, DATE_FORMAT(reported_at, 'yyyy-MM')
    HAVING SUM(least_claimed_amount_approved_amount) > 7500.0
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM summary s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
        )
      AND s.median_approved_amount <= 32500.0
),
labeled AS (
    -- Derive helper columns
    SELECT
        policy_id,
        reported_at_ym,
        median_approved_amount,
        first_reported_at,
        first_reported_at + INTERVAL 7 DAYS AS first_reported_at_plus_7d
    FROM screened
    WHERE first_reported_at < TIMESTAMP '2025-02-24 11:00:00'
),
without_matches AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM labeled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.claimed_amount < 30115.9
              AND c.claim_type <> 'delay'
        )
)
-- Final projection
SELECT
    policy_id,
    reported_at_ym,
    median_approved_amount,
    first_reported_at,
    first_reported_at_plus_7d
FROM without_matches
ORDER BY policy_id DESC NULLS FIRST, reported_at_ym
LIMIT 10
