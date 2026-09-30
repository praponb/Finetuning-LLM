WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level,
        CASE WHEN team IN ('Travel', 'Property') THEN 'group_a' WHEN team = 'Motor' THEN 'group_b' ELSE 'other' END AS team_group,
        ABS(level - 3) AS level_distance,
        LENGTH(TRIM(adjuster_name)) AS adjuster_name_len
    FROM adjusters
    WHERE adjuster_name IS NOT NULL
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.team,
        s.team_group,
        s.level_distance,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.approved_amount AS claim_approved_amount,
        c.policy_id AS claim_policy_id,
        c.claim_type
    FROM recent s
    LEFT JOIN claims c ON c.adjuster_id = s.id
),
combined_details_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.team,
        s.claim_id,
        s.claim_claimed_amount,
        s.claim_approved_amount,
        s.claim_policy_id,
        s.claim_type,
        p.start_date AS policy_start_date,
        po.holder_name
    FROM detail_rows_claims s
    JOIN policies p ON p.id = s.claim_policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM combined_details_policy
    WHERE claim_claimed_amount < (SELECT MAX(claim_claimed_amount) FROM combined_details_policy)
),
grouped AS (
    -- Group-level totals
    SELECT
        YEAR(policy_start_date) AS policy_start_date_year,
        claim_id,
        team,
        COUNT(*) AS row_count,
        ROUND(AVG(claim_claimed_amount), 2) AS avg_claim_claimed_amount,
        DATEDIFF(MAX(policy_start_date), MIN(policy_start_date)) AS policy_start_date_span_days,
        ARRAY_SORT(COLLECT_SET(holder_name)) AS holder_name_list,
        ARRAY_SORT(COLLECT_SET(team)) AS team_list,
        PERCENTILE_APPROX(claim_approved_amount, 0.5) AS median_claim_approved_amount
    FROM qualified
    WHERE claim_approved_amount > 7204.1
    GROUP BY YEAR(policy_start_date), claim_id, team
    HAVING MAX(claim_approved_amount) > 0.0
)
-- Native SQL UNPIVOT (wide to long)
SELECT policy_start_date_year, claim_id, field, value
FROM (
    SELECT policy_start_date_year, claim_id, median_claim_approved_amount, avg_claim_claimed_amount
    FROM grouped
) src
UNPIVOT (
    value FOR field IN (median_claim_approved_amount, avg_claim_claimed_amount)
) unp
ORDER BY policy_start_date_year, claim_id, field
