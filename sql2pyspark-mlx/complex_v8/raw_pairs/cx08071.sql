WITH filtered_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        reported_at
    FROM claims
    WHERE claimed_amount < 31715.7
),
claim_type_stats AS (
    -- Avg claimed_amount per claim_type
    SELECT claim_type, AVG(claimed_amount) AS avg_claimed_amount
    FROM filtered_claims
    GROUP BY claim_type
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_claimed_amount,
        ROUND(s.claimed_amount - g.avg_claimed_amount, 2) AS claimed_amount_minus_avg
    FROM filtered_claims s
    JOIN claim_type_stats g ON g.claim_type = s.claim_type
    WHERE s.claimed_amount > g.avg_claimed_amount
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        claimed_amount_minus_avg,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY reported_at) AS reported_at_dense_rank,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY adjuster_id, policy_id)) AS distinct_status_in_adjuster_id_policy_id
    FROM compared
)
-- Final projection
SELECT
    id,
    claim_type,
    reported_at,
    reported_at_dense_rank,
    ROUND(reported_at_dense_rank / NULLIF(claimed_amount, 0), 3) AS reported_at_dense_rank_per_claimed_amount
FROM sequenced
ORDER BY reported_at_dense_rank DESC NULLS FIRST, id DESC
LIMIT 10
