WITH recent_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        LAST_DAY(claim_date) AS claim_date_month_end,
        CASE WHEN claimed_amount >= 40638.4 THEN 'A' WHEN claimed_amount >= 19609.8 THEN 'B' ELSE 'C' END AS claimed_amount_tier,
        ABS(claimed_amount - 20505.7) AS claimed_amount_distance
    FROM claims
    WHERE MONTH(reported_at) IN (2, 3)
),
enriched AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.status,
        s.claim_date_month_end,
        s.claimed_amount_distance,
        p.holder_id AS policy_holder_id,
        p.end_date AS policy_end_date,
        p.status AS policy_status,
        a.level AS adjuster_level,
        a.team AS adjuster_team
    FROM recent_claims s
    JOIN policies p ON p.id = s.policy_id AND p.start_date >= DATE '2024-06-19'
    JOIN adjusters a ON a.id = s.adjuster_id
    WHERE p.holder_id >= 18
)
SELECT id, measure, value
FROM (
    SELECT id, claimed_amount_distance, claimed_amount
    FROM enriched
) src
UNPIVOT (
    value FOR measure IN (claimed_amount_distance, claimed_amount)
) unp
ORDER BY id, measure
