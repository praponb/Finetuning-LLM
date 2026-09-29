WITH scoped AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        reported_at
    FROM claims
    WHERE (claim_type <> 'flood' OR policy_id IS NOT NULL)
),
latest_per_key AS (
    -- Keep the latest row per policy_id, adjuster_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        reported_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id, adjuster_id ORDER BY claimed_amount DESC, id DESC) AS rn
        FROM scoped
    ) ranked
    WHERE rn = 1
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        reported_at,
        LEAST(approved_amount, claimed_amount) AS least_approved_amount_claimed_amount,
        reported_at + INTERVAL 1 DAYS AS reported_at_plus_1d
    FROM latest_per_key
    WHERE adjuster_id IS NOT NULL
),
adjuster_id_stats AS (
    -- Median least_approved_amount_claimed_amount per adjuster_id
    SELECT adjuster_id, PERCENTILE_APPROX(least_approved_amount_claimed_amount, 0.5) AS median_least_approved_amount_claimed_amount
    FROM labeled
    GROUP BY adjuster_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_least_approved_amount_claimed_amount,
        ROUND(s.least_approved_amount_claimed_amount - g.median_least_approved_amount_claimed_amount, 2) AS least_approved_amount_claimed_amount_minus_media
    FROM labeled s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.least_approved_amount_claimed_amount >= g.median_least_approved_amount_claimed_amount
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    approved_amount,
    reported_at,
    least_approved_amount_claimed_amount,
    reported_at_plus_1d,
    median_least_approved_amount_claimed_amount,
    least_approved_amount_claimed_amount_minus_media,
    ROUND(approved_amount * 1.1, 2) AS approved_amount_adj,
    DATE_TRUNC('DAY', reported_at) AS reported_at_day
FROM compared
WHERE HOUR(reported_at) BETWEEN 10 AND 13
ORDER BY least_approved_amount_claimed_amount_minus_media NULLS LAST, approved_amount DESC NULLS FIRST
