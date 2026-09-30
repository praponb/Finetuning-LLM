WITH scoped AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        reported_at,
        YEAR(reported_at) AS reported_at_year,
        DATEDIFF(DATE '2025-12-31', reported_at) AS days_since_reported_at,
        claimed_amount - approved_amount AS claimed_amount_minus_approved_amount
    FROM claims
    WHERE claim_type NOT IN ('flood', 'fire')
),
adjuster_id_stats AS (
    -- Avg claimed_amount_minus_approved_amount per adjuster_id
    SELECT adjuster_id, AVG(claimed_amount_minus_approved_amount) AS avg_claimed_amount_minus_approved_amount
    FROM scoped
    GROUP BY adjuster_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_claimed_amount_minus_approved_amount,
        ROUND(s.claimed_amount_minus_approved_amount - g.avg_claimed_amount_minus_approved_amount, 2) AS claimed_amount_minus_approved_amount_minus_avg
    FROM scoped s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.claimed_amount_minus_approved_amount > g.avg_claimed_amount_minus_approved_amount
),
without_matches AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM above_average s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.status IS NOT NULL
              AND c.claim_type NOT IN ('theft', 'collision')
        )
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        policy_id,
        claim_type,
        claimed_amount,
        reported_at_year,
        claimed_amount_minus_approved_amount,
        avg_claimed_amount_minus_approved_amount,
        DATE_FORMAT(reported_at, 'yyyy-MM-dd') AS reported_at_label,
        ROUND(claimed_amount * 1.1, 2) AS claimed_amount_adj,
        SUBSTRING(TRIM(claim_type), 1, 3) AS claim_type_prefix
    FROM without_matches
),
with_details AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.claim_type,
        s.claimed_amount,
        s.reported_at_year,
        s.claimed_amount_minus_approved_amount,
        s.avg_claimed_amount_minus_approved_amount,
        s.reported_at_label,
        s.claimed_amount_adj,
        s.claim_type_prefix,
        p.holder_id AS policy_holder_id,
        p.premium AS policy_premium,
        po.segment AS holder_segment,
        po.holder_name
    FROM scored s
    LEFT JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.claim_type,
        s.claimed_amount,
        s.avg_claimed_amount_minus_approved_amount,
        s.reported_at_label,
        s.claimed_amount_adj,
        s.claim_type_prefix,
        s.policy_holder_id,
        s.policy_premium,
        s.holder_segment,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.claim_type AS claim_claim_type,
        c.approved_amount AS claim_approved_amount,
        c.claimed_amount AS claim_claimed_amount
    FROM with_details s
    JOIN claims c ON c.policy_id = s.policy_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(claimed_amount, 2) OVER (PARTITION BY claim_id, claim_claim_type ORDER BY claimed_amount, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claimed_amount,
        claimed_amount_adj - LAG(claimed_amount_adj) OVER w_claim_id_claim_claim_type_by_2 AS claimed_amount_adj_change,
        MIN(claim_claimed_amount) OVER w_claim_type_claim_claim_type AS claim_type_claim_claim_type_min_claim_claimed_am,
        SUM(claimed_amount) OVER (PARTITION BY claim_type, claim_claim_type) AS claim_type_claim_claim_type_sum_claimed_amount,
        ROUND(AVG(claim_claimed_amount) OVER (PARTITION BY claim_type, claim_claim_type), 2) AS claim_type_claim_claim_type_avg_claim_claimed_am,
        SUM(claimed_amount) OVER (PARTITION BY claim_id ORDER BY avg_claimed_amount_minus_approved_amount DESC, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount
    FROM detail_rows_claims
    WINDOW w_claim_id_claim_claim_type_by_2 AS (PARTITION BY claim_id, claim_claim_type ORDER BY claimed_amount, id, claim_id), w_claim_type_claim_claim_type AS (PARTITION BY claim_type, claim_claim_type)
)
-- Final projection
SELECT
    id,
    claim_type,
    claimed_amount,
    claimed_amount_adj,
    claim_type_prefix,
    policy_premium,
    claim_approved_amount,
    claim_claimed_amount,
    second_claimed_amount,
    claimed_amount_adj_change,
    claim_type_claim_claim_type_min_claim_claimed_am,
    claim_type_claim_claim_type_avg_claim_claimed_am,
    ROUND(second_claimed_amount / NULLIF(claim_claimed_amount, 0), 3) AS second_claimed_amount_per_claim_claimed_amount
FROM with_windows
ORDER BY claimed_amount DESC, claimed_amount_adj_change DESC
