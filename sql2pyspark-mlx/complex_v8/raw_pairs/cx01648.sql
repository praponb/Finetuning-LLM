WITH base AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        reported_at,
        DATEDIFF(DATE '2025-06-30', claim_date) AS days_since_claim_date,
        ABS(approved_amount - 10355.0) AS approved_amount_distance,
        claimed_amount + approved_amount AS claimed_amount_plus_approved_amount
    FROM claims
    WHERE adjuster_id >= 6
      AND policy_id >= 27
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        adjuster_id,
        claimed_amount,
        days_since_claim_date,
        approved_amount_distance,
        claimed_amount_plus_approved_amount,
        LEAST(approved_amount_distance, days_since_claim_date) AS least_approved_amount_distance_days_since_claim_,
        SUBSTRING(TRIM(claim_type), 1, 2) AS claim_type_prefix
    FROM base
),
latest AS (
    -- Keep the latest row per adjuster_id
    SELECT
        id,
        adjuster_id,
        claimed_amount,
        days_since_claim_date,
        approved_amount_distance,
        claimed_amount_plus_approved_amount,
        least_approved_amount_distance_days_since_claim_,
        claim_type_prefix
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id ORDER BY approved_amount_distance DESC, id DESC) AS recency_rank
        FROM scored
    ) ranked
    WHERE recency_rank = 1
),
adjuster_id_stats AS (
    -- Avg claimed_amount per adjuster_id
    SELECT adjuster_id, AVG(claimed_amount) AS avg_claimed_amount
    FROM latest
    GROUP BY adjuster_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_claimed_amount,
        ROUND(s.claimed_amount - g.avg_claimed_amount, 2) AS claimed_amount_minus_avg
    FROM latest s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.claimed_amount > g.avg_claimed_amount
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.adjuster_id,
        s.days_since_claim_date,
        s.approved_amount_distance,
        s.claimed_amount_plus_approved_amount,
        s.least_approved_amount_distance_days_since_claim_,
        s.avg_claimed_amount,
        s.claimed_amount_minus_avg,
        c.id AS claim_id,
        c.policy_id AS claim_policy_id,
        c.claim_type
    FROM above_average s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
),
scored_2 AS (
    -- Add calculated columns
    SELECT
        id,
        days_since_claim_date,
        approved_amount_distance,
        claimed_amount_plus_approved_amount,
        least_approved_amount_distance_days_since_claim_,
        claimed_amount_minus_avg,
        claim_id,
        claim_policy_id,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown,
        LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug,
        ABS(claimed_amount_minus_avg - -10000.0) AS claimed_amount_minus_avg_distance
    FROM exploded_detail
),
joined AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.approved_amount_distance,
        s.claimed_amount_plus_approved_amount,
        s.least_approved_amount_distance_days_since_claim_,
        s.claimed_amount_minus_avg,
        s.claim_id,
        s.claim_policy_id,
        s.claim_type_or_unknown,
        s.claimed_amount_minus_avg_distance,
        p.start_date AS policy_start_date,
        p.premium AS policy_premium,
        p.product AS policy_product,
        c.status AS claim_status
    FROM scored_2 s
    JOIN policies p ON p.id = s.claim_policy_id
    LEFT JOIN claims c ON c.id = s.claim_id
)
-- Final projection
SELECT
    id,
    approved_amount_distance,
    claimed_amount_plus_approved_amount,
    least_approved_amount_distance_days_since_claim_,
    claimed_amount_minus_avg,
    claim_type_or_unknown,
    claimed_amount_minus_avg_distance,
    policy_start_date,
    CONCAT(claim_status, '#', CAST(id AS STRING)) AS claim_status_ref
FROM joined
ORDER BY claimed_amount_minus_avg_distance
