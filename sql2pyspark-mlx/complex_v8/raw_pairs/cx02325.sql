WITH recent_claims AS (
    -- Scope the claims rows
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
        INITCAP(TRIM(status)) AS status_clean
    FROM claims
    WHERE adjuster_id IS NOT NULL
      AND reported_at >= TIMESTAMP '2025-01-25 14:00:00'
),
enriched AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.status_clean,
        a.team AS adjuster_team,
        a.adjuster_name,
        a.level AS adjuster_level
    FROM recent_claims s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
joined_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.status_clean,
        s.adjuster_name,
        a.level AS adjuster_level_2
    FROM enriched s
    JOIN adjusters a ON a.id = s.adjuster_id
)
SELECT
    COALESCE(claim_type, 'ALL') AS claim_type,
    policy_id,
    GROUPING(claim_type) AS is_total_claim_type,
    COUNT(*) AS cnt,
    SUM(adjuster_level_2) AS total_adjuster_level_2,
    SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) AS n_status_open
FROM joined_adjuster
GROUP BY GROUPING SETS ((claim_type, policy_id), (claim_type), (policy_id), ())
ORDER BY claim_type NULLS LAST, policy_id NULLS LAST
