WITH base_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        ROUND(claimed_amount * 1.5, 2) AS claimed_amount_adj
    FROM claims
    WHERE approved_amount BETWEEN 0.0 AND 25402.3
      AND policy_id <= 29
),
qualified AS (
    -- Keep rows whose policy matches
    SELECT s.*
    FROM base_claims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.id = s.policy_id
              AND p.product IN ('TRAVEL', 'AUTO')
        )
),
tagged AS (
    -- Derive helper columns
    SELECT *, YEAR(claim_date) AS claim_date_year
    FROM qualified
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY policy_id ORDER BY claim_date_year DESC), 3) AS claim_date_year_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY policy_id ORDER BY claim_date_year DESC), 3) AS claim_date_year_cume_dist,
        MAX(claimed_amount_adj) OVER (PARTITION BY claim_date_year) AS claim_date_year_max_claimed_amount_adj,
        FIRST_VALUE(claimed_amount) OVER (PARTITION BY policy_id ORDER BY claim_date_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claimed_amount,
        LAST_VALUE(claim_date_year) IGNORE NULLS OVER (PARTITION BY policy_id ORDER BY claim_date_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date_year,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY claim_date_year)) AS distinct_status_in_claim_date_year
    FROM tagged
),
with_details_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.claimed_amount_adj,
        s.claim_date_year_cume_dist,
        s.claim_date_year_max_claimed_amount_adj,
        s.first_claimed_amount,
        s.last_known_claim_date_year,
        s.distinct_status_in_claim_date_year,
        p.holder_id AS policy_holder_id,
        a.level AS adjuster_level
    FROM windowed s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
screened AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM with_details_policy s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.policy_holder_id
              AND p.premium <= 3817.5
              AND p.product = 'LIFE'
        )
      AND s.claimed_amount_adj BETWEEN 22570.0 AND 60020.0
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_type,
    claimed_amount,
    claimed_amount_adj,
    claim_date_year_cume_dist,
    claim_date_year_max_claimed_amount_adj,
    first_claimed_amount,
    distinct_status_in_claim_date_year,
    policy_holder_id
FROM screened
ORDER BY claimed_amount NULLS LAST
