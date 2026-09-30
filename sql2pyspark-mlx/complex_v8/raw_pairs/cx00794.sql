WITH cleaned_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        CONCAT(claim_type, '#', CAST(policy_id AS STRING)) AS claim_type_ref,
        ABS(approved_amount - 4868.7) AS approved_amount_distance,
        ABS(claimed_amount - 29336.0) AS claimed_amount_distance
    FROM claims
    WHERE (HOUR(reported_at) BETWEEN 9 AND 11 OR claim_type = 'theft')
),
latest AS (
    -- Keep the latest row per policy_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        claim_type_ref,
        approved_amount_distance,
        claimed_amount_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY approved_amount_distance DESC, id DESC) AS recency_rank
        FROM cleaned_claims
    ) ranked
    WHERE recency_rank = 1
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        reported_at,
        approved_amount_distance,
        DATE_FORMAT(reported_at, 'yyyy-MM') AS reported_at_label
    FROM latest
),
policy_id_stats AS (
    -- Median approved_amount_distance per policy_id
    SELECT policy_id, PERCENTILE_APPROX(approved_amount_distance, 0.5) AS median_approved_amount_distance
    FROM tagged
    GROUP BY policy_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_approved_amount_distance,
        ROUND(s.approved_amount_distance - g.median_approved_amount_distance, 2) AS approved_amount_distance_minus_median
    FROM tagged s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.approved_amount_distance >= g.median_approved_amount_distance
)
SELECT
    id,
    policy_id,
    claim_date,
    claim_type,
    reported_at,
    approved_amount_distance,
    reported_at_label,
    median_approved_amount_distance,
    DATE_FORMAT(reported_at, 'MMM yyyy') AS reported_at_label_2,
    CONCAT(claim_type, '#', CAST(policy_id AS STRING)) AS claim_type_ref
FROM vs_group
WHERE median_approved_amount_distance >= 17500.0
ORDER BY reported_at_label_2 DESC, reported_at DESC, id
LIMIT 20
