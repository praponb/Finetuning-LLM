WITH base AS (
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
    WHERE approved_amount > 492.6
      AND policy_id IS NOT NULL
),
rollup_base AS (
    -- Group-level totals
    SELECT
        YEAR(reported_at) AS reported_at_year,
        policy_id,
        status,
        ROUND(SUM(approved_amount), 2) AS total_approved_amount,
        MAX(claimed_amount) AS max_claimed_amount,
        ROUND(100 * COUNT(CASE WHEN claim_type IN ('flood', 'delay', 'theft') THEN 1 END) / COUNT(*), 1) AS pct_claim_type_in_flood_delay_th
    FROM base
    GROUP BY YEAR(reported_at), policy_id, status
    HAVING AVG(claimed_amount) > 11235.6
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM rollup_base s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.claim_date >= DATE '2024-03-30'
        )
),
combined_details AS (
    -- Join policies
    SELECT
        s.reported_at_year,
        s.policy_id,
        s.status,
        s.max_claimed_amount,
        s.pct_claim_type_in_flood_delay_th,
        p.holder_id AS policy_holder_id
    FROM with_activity s
    JOIN policies p ON p.id = s.policy_id
)
-- Final projection
SELECT
    reported_at_year,
    policy_id,
    status,
    pct_claim_type_in_flood_delay_th,
    policy_holder_id,
    ROUND(max_claimed_amount * 1.1, 2) AS max_claimed_amount_adj
FROM combined_details
ORDER BY max_claimed_amount_adj, pct_claim_type_in_flood_delay_th
