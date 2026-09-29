WITH prepared AS (
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
        CASE WHEN claimed_amount >= 34401.0 THEN 'large' WHEN claimed_amount >= 7204.1 THEN 'medium' ELSE 'small' END AS claimed_amount_tier,
        DATE_ADD(claim_date, 30) AS claim_date_plus_30d,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket
    FROM claims
    WHERE MONTH(reported_at) IN (2, 3)
      AND adjuster_id <= 2
),
adjuster_id_stats AS (
    -- Median claimed_amount per adjuster_id
    SELECT adjuster_id, PERCENTILE_APPROX(claimed_amount, 0.5) AS median_claimed_amount
    FROM prepared
    GROUP BY adjuster_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_claimed_amount,
        ROUND(s.claimed_amount - g.median_claimed_amount, 2) AS claimed_amount_minus_median
    FROM prepared s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.claimed_amount >= g.median_claimed_amount
),
combined_details AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.claimed_amount_tier,
        s.claim_date_plus_30d,
        s.claimed_amount_minus_median,
        a.adjuster_name,
        a.level AS adjuster_level,
        p.premium AS policy_premium,
        p.end_date AS policy_end_date
    FROM above_average s
    JOIN adjusters a ON a.id = s.adjuster_id
    JOIN policies p ON p.id = s.policy_id
    WHERE UPPER(TRIM(a.adjuster_name)) LIKE 'H%'
),
latest AS (
    -- Keep the latest row per claimed_amount_tier, status
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
        claimed_amount_tier,
        claim_date_plus_30d,
        claimed_amount_minus_median,
        adjuster_name,
        adjuster_level,
        policy_premium,
        policy_end_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claimed_amount_tier, status ORDER BY claim_date DESC, id DESC) AS row_num
        FROM combined_details
    ) ranked
    WHERE row_num = 1
)
-- Subtotals with CUBE
SELECT
    COALESCE(status, 'ALL') AS status,
    adjuster_level,
    SUM(policy_premium) FILTER (WHERE claim_type <> 'fire') AS policy_premium_claim_type_fire,
    ROUND(SUM(claimed_amount) / NULLIF(SUM(adjuster_level), 0), 3) AS claimed_amount_to_adjuster_level_ratio
FROM latest
GROUP BY CUBE(status, adjuster_level)
ORDER BY status NULLS LAST, adjuster_level NULLS LAST
