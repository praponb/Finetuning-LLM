WITH recent AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        CASE WHEN claimed_amount >= 30115.9 THEN 'A' WHEN claimed_amount >= 10518.4 THEN 'B' ELSE 'C' END AS claimed_amount_tier,
        DATE_TRUNC('QUARTER', reported_at) AS reported_at_quarter,
        UPPER(TRIM(claim_type)) AS claim_type_clean
    FROM claims
    WHERE status = 'IN_REVIEW'
),
combined_details_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.reported_at,
        s.claimed_amount_tier,
        s.reported_at_quarter,
        s.claim_type_clean,
        p.holder_id AS policy_holder_id,
        a.adjuster_name,
        a.team AS adjuster_team
    FROM recent s
    LEFT JOIN policies p ON p.id = s.policy_id
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
enriched AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.status,
        s.reported_at,
        s.reported_at_quarter,
        s.claim_type_clean,
        s.policy_holder_id,
        a.adjuster_name AS adjuster_adjuster_name,
        a.level AS adjuster_level,
        a.team AS adjuster_team_2
    FROM combined_details_policy s
    JOIN adjusters a ON a.id = s.adjuster_id AND LENGTH(TRIM(a.adjuster_name)) >= 8
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT policy_id, status, adjuster_level
    FROM enriched
) src
PIVOT (
    SUM(adjuster_level)
    FOR status IN ('REJECTED', 'APPROVED')
)
ORDER BY policy_id
