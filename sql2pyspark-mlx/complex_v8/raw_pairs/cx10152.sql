WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at
    FROM claims
    WHERE policy_id IS NOT NULL
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY adjuster_id, claim_type)) AS distinct_policy_id_in_adjuster_id_claim_type,
        NTH_VALUE(reported_at, 2) OVER (PARTITION BY adjuster_id, MONTH(reported_at) ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at,
        ROUND(AVG(approved_amount) OVER (PARTITION BY adjuster_id, MONTH(reported_at) ORDER BY approved_amount DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_1_prior,
        DENSE_RANK() OVER w_adjuster_id_reported_at_mont_3 AS approved_amount_dense_rank,
        LEAD(reported_at) OVER (PARTITION BY adjuster_id, MONTH(reported_at) ORDER BY approved_amount DESC, id) AS next_reported_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY adjuster_id, MONTH(reported_at) ORDER BY approved_amount DESC), 3) AS approved_amount_percent_rank
    FROM filtered
    WINDOW w_adjuster_id_reported_at_mont_3 AS (PARTITION BY adjuster_id, MONTH(reported_at) ORDER BY approved_amount DESC, id)
),
adjuster_id_stats AS (
    -- Median approved_amount_avg_1_prior per adjuster_id
    SELECT adjuster_id, PERCENTILE_APPROX(approved_amount_avg_1_prior, 0.5) AS median_approved_amount_avg_1_prior
    FROM metrics
    GROUP BY adjuster_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_approved_amount_avg_1_prior,
        ROUND(s.approved_amount_avg_1_prior - g.median_approved_amount_avg_1_prior, 2) AS approved_amount_avg_1_prior_minus_median
    FROM metrics s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.approved_amount_avg_1_prior >= g.median_approved_amount_avg_1_prior
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM compared s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
        )
)
-- Subtotals with GROUPING SETS
SELECT
    claim_type,
    approved_amount_dense_rank,
    distinct_policy_id_in_adjuster_id_claim_type,
    MIN(next_reported_at) AS first_next_reported_at,
    ROUND(100 * COUNT(CASE WHEN claim_type IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_claim_type_is_not_null,
    ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list
FROM with_activity
GROUP BY GROUPING SETS ((claim_type, approved_amount_dense_rank, distinct_policy_id_in_adjuster_id_claim_type), (claim_type), (approved_amount_dense_rank))
ORDER BY claim_type NULLS LAST, approved_amount_dense_rank NULLS LAST, distinct_policy_id_in_adjuster_id_claim_type NULLS LAST
