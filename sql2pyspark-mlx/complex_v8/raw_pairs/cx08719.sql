WITH prepared_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug
    FROM claims
    WHERE approved_amount BETWEEN 0.0 AND 20880.4
      AND status IN ('OPEN', 'PAID')
),
adjuster_id_stats AS (
    -- Median claimed_amount per adjuster_id
    SELECT adjuster_id, PERCENTILE_APPROX(claimed_amount, 0.5) AS median_claimed_amount
    FROM prepared_claims
    GROUP BY adjuster_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_claimed_amount,
        ROUND(s.claimed_amount - g.median_claimed_amount, 2) AS claimed_amount_minus_median
    FROM prepared_claims s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.claimed_amount >= g.median_claimed_amount
),
with_parent_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.claim_type_slug,
        s.claimed_amount_minus_median,
        a.team AS adjuster_team,
        a.adjuster_name
    FROM vs_group s
    JOIN adjusters a ON a.id = s.adjuster_id
)
SELECT id, measure, reading
FROM (
    SELECT id, approved_amount, claimed_amount_minus_median
    FROM with_parent_adjuster
) src
UNPIVOT (
    reading FOR measure IN (approved_amount, claimed_amount_minus_median)
) unp
