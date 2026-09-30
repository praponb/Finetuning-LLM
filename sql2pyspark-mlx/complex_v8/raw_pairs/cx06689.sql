WITH base_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        TO_DATE(reported_at) AS reported_at_date,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix
    FROM claims
    WHERE claim_date BETWEEN DATE '2024-02-23' AND DATE '2025-11-12'
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.claimed_amount) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS min_claim_claimed_amount,
        (SELECT COUNT(*) FILTER (WHERE c.claim_type <> 'delay') FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_claim_type_count,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS first_claim_reported_at
    FROM base_claims s
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.reported_at_date,
        s.status_prefix,
        s.min_claim_claimed_amount,
        s.claim_claim_type_count,
        s.first_claim_reported_at,
        a.level AS adjuster_level,
        a.team AS adjuster_team,
        p.status AS policy_status,
        p.premium AS policy_premium
    FROM with_stats s
    JOIN adjusters a ON a.id = s.adjuster_id
    JOIN policies p ON p.id = s.policy_id
),
stats AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', reported_at_date) AS reported_at_date_month,
        claim_claim_type_count,
        MAX(min_claim_claimed_amount) AS max_min_claim_claimed_amount,
        SUM(CASE WHEN claim_type = 'collision' THEN 1 ELSE 0 END) AS n_claim_type_collision,
        ROUND(STDDEV(claim_claim_type_count), 2) AS stddev_claim_claim_type_count
    FROM with_parent
    GROUP BY DATE_TRUNC('MONTH', reported_at_date), claim_claim_type_count
    HAVING AVG(adjuster_level) > 2
)
SELECT DISTINCT stddev_claim_claim_type_count
FROM stats
ORDER BY stddev_claim_claim_type_count NULLS LAST
