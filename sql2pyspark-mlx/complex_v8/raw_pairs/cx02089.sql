WITH cleaned_claims AS (
    -- Filter claims
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
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        UPPER(TRIM(claim_type)) AS claim_type_clean,
        YEAR(claim_date) AS claim_date_year
    FROM claims
    WHERE claim_type = 'flood'
      AND status = 'PAID'
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
        s.approved_amount,
        s.reported_at,
        s.claim_type_clean,
        s.claim_date_year,
        a.adjuster_name,
        a.team AS adjuster_team,
        a.level AS adjuster_level,
        p.premium AS policy_premium,
        p.end_date AS policy_end_date
    FROM cleaned_claims s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
    JOIN policies p ON p.id = s.policy_id
)
-- Subtotals with CUBE
SELECT
    adjuster_team,
    adjuster_id,
    COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids,
    MIN(claim_date) AS first_claim_date
FROM joined_adjuster
GROUP BY CUBE(adjuster_team, adjuster_id)
