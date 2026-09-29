WITH cleaned_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        LAST_DAY(claim_date) AS claim_date_month_end,
        LEAST(approved_amount, claimed_amount) AS least_approved_amount_claimed_amount,
        MONTH(claim_date) AS claim_date_month
    FROM claims
    WHERE claim_date BETWEEN DATE '2024-05-14' AND DATE '2025-04-25'
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        least_approved_amount_claimed_amount,
        approved_amount - claim_date_month AS approved_amount_minus_claim_date_month,
        SUBSTRING(TRIM(claim_type), 1, 2) AS claim_type_prefix,
        CASE WHEN claimed_amount >= 29336.0 THEN 'HIGH' WHEN claimed_amount >= 14019.8 THEN 'MEDIUM' ELSE 'LOW' END AS claimed_amount_tier
    FROM cleaned_claims
),
enriched_adjuster AS (
    -- Attach adjuster attributes
    SELECT s.*, a.team AS adjuster_team
    FROM labeled s
    JOIN adjusters a ON a.id = s.adjuster_id
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM enriched_adjuster
    WHERE approved_amount_minus_claim_date_month > (SELECT AVG(approved_amount_minus_claim_date_month) FROM enriched_adjuster)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY claim_type)) AS distinct_adjuster_id_in_claim_type,
        MAX(claimed_amount) OVER (PARTITION BY policy_id, adjuster_team ORDER BY claimed_amount DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS claimed_amount_max_4
    FROM with_activity
),
with_parent_adjuster AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.reported_at,
        s.least_approved_amount_claimed_amount,
        s.approved_amount_minus_claim_date_month,
        s.claimed_amount_tier,
        s.adjuster_team,
        s.distinct_adjuster_id_in_claim_type,
        s.claimed_amount_max_4,
        a.team AS adjuster_team_2
    FROM windowed s
    JOIN adjusters a ON a.id = s.adjuster_id
)
SELECT
    id,
    policy_id,
    adjuster_id,
    approved_amount,
    reported_at,
    approved_amount_minus_claim_date_month,
    adjuster_team,
    distinct_adjuster_id_in_claim_type,
    claimed_amount_max_4,
    CASE WHEN claimed_amount >= 41760.8 THEN 'HIGH' WHEN claimed_amount >= 19609.8 THEN 'MEDIUM' ELSE 'LOW' END AS claimed_amount_tier_2,
    ADD_MONTHS(claim_date, 6) AS claim_date_plus_6m
FROM with_parent_adjuster
WHERE adjuster_team_2 = 'Travel'
ORDER BY reported_at DESC, id NULLS LAST
