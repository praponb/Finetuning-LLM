WITH filtered AS (
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
        ROUND(approved_amount * 0.25, 2) AS approved_amount_adj
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 6 AND 11
      AND adjuster_id <= 6
),
filtered_2 AS (
    -- Keep rows whose adjuster matches
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM adjusters a
            WHERE a.id = s.adjuster_id
              AND a.level BETWEEN 1 AND 3
        )
),
tagged AS (
    -- Add calculated columns
    SELECT *, ADD_MONTHS(claim_date, 3) AS claim_date_plus_3m
    FROM filtered_2
),
with_activity AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM tagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.policy_id IS NOT NULL
              AND c.claim_date >= DATE '2024-06-07'
        )
)
SELECT id, field, value
FROM (
    SELECT id, claimed_amount, approved_amount_adj
    FROM with_activity
) src
UNPIVOT (
    value FOR field IN (claimed_amount, approved_amount_adj)
) unp
