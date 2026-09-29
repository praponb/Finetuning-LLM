WITH recent AS (
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
        reported_at
    FROM claims
    WHERE claimed_amount > 4554.8
      AND adjuster_id >= 4
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.reported_at,
        c.id AS claim_id,
        c.status AS claim_status,
        c.adjuster_id AS claim_adjuster_id,
        c.claim_type AS claim_claim_type,
        c.approved_amount AS claim_approved_amount
    FROM recent s
    JOIN claims c ON c.policy_id = s.policy_id
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM exploded_detail s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.claim_adjuster_id
              AND c.reported_at < TIMESTAMP '2025-03-08 10:00:00'
        )
),
per_group AS (
    -- Summarise per group
    SELECT
        TO_DATE(reported_at) AS reported_at_day,
        claim_id,
        PERCENTILE_APPROX(approved_amount, 0.5) AS median_approved_amount,
        ROUND(100 * COUNT(CASE WHEN claim_status IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_claim_status_is_not_null,
        ROUND(SUM(approved_amount), 2) AS total_approved_amount,
        COUNT(*) AS row_count
    FROM with_activity
    WHERE adjuster_id IS NOT NULL
    GROUP BY TO_DATE(reported_at), claim_id
),
filtered AS (
    -- Keep rows whose claim matches
    SELECT s.*
    FROM per_group s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.id = s.claim_id
              AND c.status <> 'REJECTED'
              AND c.claim_date < DATE '2025-03-31'
        )
)
SELECT
    reported_at_day,
    claim_id,
    median_approved_amount,
    total_approved_amount,
    row_count
FROM filtered
