WITH valid_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        ADD_MONTHS(claim_date, 12) AS claim_date_plus_12m,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled,
        HOUR(reported_at) AS reported_at_hour
    FROM claims
    WHERE policy_id IS NOT NULL
      AND claim_date >= DATE '2024-07-01'
),
without_matches AS (
    -- Keep rows whose adjuster matches
    SELECT s.*
    FROM valid_claims s
    WHERE EXISTS (
            SELECT 1
            FROM adjusters a
            WHERE a.id = s.adjuster_id
              AND a.team IN ('Health', 'Property')
              AND a.level > 2
        )
)
-- Subtotals with CUBE
SELECT
    reported_at_hour,
    claim_type,
    policy_id,
    ROUND(SUM(claimed_amount), 2) AS total_claimed_amount,
    ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
    MAX(claim_date) AS last_claim_date
FROM without_matches
GROUP BY CUBE(reported_at_hour, claim_type, policy_id)
