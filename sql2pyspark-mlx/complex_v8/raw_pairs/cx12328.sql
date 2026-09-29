WITH filtered AS (
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
        DATEDIFF(DATE '2026-01-01', claim_date) AS days_since_claim_date
    FROM claims
    WHERE claimed_amount BETWEEN 5313.8 AND 43724.9
      AND reported_at < TIMESTAMP '2025-02-18 13:00:00'
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.days_since_claim_date,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.claim_type AS claim_claim_type
    FROM filtered s
    LEFT JOIN claims c ON c.adjuster_id = s.adjuster_id
),
joined_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.days_since_claim_date,
        s.claim_id,
        s.claim_reported_at,
        p.premium AS policy_premium,
        p.start_date AS policy_start_date
    FROM detail_rows s
    JOIN policies p ON p.id = s.policy_id
),
latest_per_key AS (
    -- Keep the latest row per adjuster_id, policy_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        days_since_claim_date,
        claim_id,
        claim_reported_at,
        policy_premium,
        policy_start_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id, policy_id ORDER BY policy_start_date DESC, id DESC, claim_id DESC) AS recency_rank
        FROM joined_policy
    ) ranked
    WHERE recency_rank = 1
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        claimed_amount,
        days_since_claim_date,
        claim_id,
        claim_reported_at,
        policy_premium,
        policy_start_date,
        DATE_ADD(claim_date, 30) AS claim_date_plus_30d,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket
    FROM latest_per_key
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN days_since_claim_date >= 490 THEN 'A' WHEN days_since_claim_date >= 245 THEN 'B' ELSE 'C' END AS days_since_claim_date_tier,
        ABS(policy_premium - 2289.0) AS policy_premium_distance
    FROM tagged
),
grouped AS (
    -- Summarise per group
    SELECT
        days_since_claim_date_tier,
        ROUND(STDDEV(claimed_amount_bucket), 2) AS stddev_claimed_amount_bucket,
        COUNT(*) AS row_count,
        MAX(claim_date_plus_30d) AS last_claim_date_plus_30d,
        COUNT(DISTINCT id) AS distinct_ids,
        MIN(days_since_claim_date) AS min_days_since_claim_date
    FROM derived
    WHERE claim_date_plus_30d IS NOT NULL
    GROUP BY days_since_claim_date_tier
    HAVING SUM(days_since_claim_date) > 105
)
-- Final projection
SELECT
    days_since_claim_date_tier,
    stddev_claimed_amount_bucket,
    row_count,
    min_days_since_claim_date,
    QUARTER(last_claim_date_plus_30d) AS last_claim_date_plus_30d_quarter
FROM grouped
WHERE stddev_claimed_amount_bucket > 17500.0
ORDER BY row_count DESC
