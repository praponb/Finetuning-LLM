WITH cleaned_claims AS (
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
        DATE_TRUNC('DAY', reported_at) AS reported_at_day
    FROM claims
    WHERE policy_id >= 26
      AND claim_date BETWEEN DATE '2024-06-26' AND DATE '2025-09-14'
),
all_events AS (
    -- Stack the branches
    SELECT id AS qty, claim_date, approved_amount
    FROM cleaned_claims
    WHERE policy_id IS NOT NULL
      AND adjuster_id IS NOT NULL
    UNION
    SELECT adjuster_id, claim_date, claimed_amount
    FROM claims
)
SELECT qty, COUNT(*) AS n_rows, SUM(approved_amount) AS total_approved_amount, MAX(claim_date) AS latest_claim_date
FROM all_events
GROUP BY qty
ORDER BY qty
