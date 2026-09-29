WITH filtered_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        reported_at,
        claimed_amount - approved_amount AS claimed_amount_minus_approved_amount
    FROM claims
    WHERE reported_at < TIMESTAMP '2025-02-12 12:00:00'
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT adjuster_id, MAX(reported_at) AS last_claim_reported_at
    FROM claims
    GROUP BY adjuster_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.last_claim_reported_at
    FROM filtered_claims s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.reported_at,
        s.claimed_amount_minus_approved_amount,
        s.last_claim_reported_at,
        c.id AS claim_id,
        c.policy_id AS claim_policy_id,
        c.approved_amount AS claim_approved_amount
    FROM with_stats s
    LEFT JOIN claims c ON c.adjuster_id = s.adjuster_id
),
claim_type_stats AS (
    -- Avg claim_approved_amount per claim_type
    SELECT claim_type, AVG(claim_approved_amount) AS avg_claim_approved_amount
    FROM line_level_claims
    GROUP BY claim_type
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_claim_approved_amount,
        ROUND(s.claim_approved_amount - g.avg_claim_approved_amount, 2) AS claim_approved_amount_minus_avg
    FROM line_level_claims s
    JOIN claim_type_stats g ON g.claim_type = s.claim_type
    WHERE s.claim_approved_amount > g.avg_claim_approved_amount
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        adjuster_id,
        claimed_amount,
        last_claim_reported_at,
        claim_id,
        avg_claim_approved_amount,
        LAST_VALUE(claim_approved_amount) OVER (PARTITION BY policy_id ORDER BY avg_claim_approved_amount ASC, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claim_approved_amount,
        DENSE_RANK() OVER w_policy_id_by_avg_claim_appro_2 AS avg_claim_approved_amount_dense_rank,
        ROUND(claim_approved_amount - AVG(claim_approved_amount) OVER w_claim_policy_id, 2) AS claim_approved_amount_vs_claim_policy_id_avg,
        COUNT(*) OVER w_claim_policy_id AS rows_in_claim_policy_id
    FROM above_average
    WINDOW w_policy_id_by_avg_claim_appro_2 AS (PARTITION BY policy_id ORDER BY avg_claim_approved_amount ASC), w_claim_policy_id AS (PARTITION BY claim_policy_id)
)
-- Final projection
SELECT DISTINCT rows_in_claim_policy_id
FROM windowed
WHERE last_claim_approved_amount <= 16404.6
ORDER BY rows_in_claim_policy_id DESC
