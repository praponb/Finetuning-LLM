WITH base_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        reported_at,
        SUBSTRING(TRIM(claim_type), 1, 2) AS claim_type_prefix,
        LOWER(TRIM(claim_type)) AS claim_type_clean,
        CASE WHEN claim_type IN ('theft', 'medical') THEN 'group_a' WHEN claim_type = 'collision' THEN 'group_b' ELSE 'other' END AS claim_type_group
    FROM claims
    WHERE MONTH(reported_at) IN (1, 2)
      AND claim_date BETWEEN DATE '2024-02-17' AND DATE '2025-04-14'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        reported_at,
        claim_type_prefix,
        claim_type_group,
        COUNT(*) OVER (PARTITION BY adjuster_id, policy_id) AS rows_in_adjuster_id_policy_id,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY adjuster_id ORDER BY claimed_amount DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS claimed_amount_avg_6_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY adjuster_id ORDER BY approved_amount NULLS LAST), 3) AS approved_amount_percent_rank,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY adjuster_id, policy_id), 2) AS claimed_amount_vs_adjuster_id_policy_id_avg,
        LAG(claimed_amount) OVER (ORDER BY approved_amount, id) AS prev_claimed_amount,
        LAG(claimed_amount, 3) OVER (ORDER BY approved_amount, id) AS prev3_claimed_amount
    FROM base_claims
),
claim_type_group_stats AS (
    -- Avg claimed_amount_avg_6_prior per claim_type_group
    SELECT claim_type_group, AVG(claimed_amount_avg_6_prior) AS avg_claimed_amount_avg_6_prior
    FROM sequenced
    GROUP BY claim_type_group
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_claimed_amount_avg_6_prior,
        ROUND(s.claimed_amount_avg_6_prior - g.avg_claimed_amount_avg_6_prior, 2) AS claimed_amount_avg_6_prior_minus_avg
    FROM sequenced s
    JOIN claim_type_group_stats g ON g.claim_type_group = s.claim_type_group
    WHERE s.claimed_amount_avg_6_prior > g.avg_claimed_amount_avg_6_prior
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, value
FROM (
    SELECT id, claimed_amount_avg_6_prior_minus_avg, prev3_claimed_amount, claimed_amount_vs_adjuster_id_policy_id_avg, avg_claimed_amount_avg_6_prior
    FROM compared
) src
UNPIVOT (
    value FOR metric IN (claimed_amount_avg_6_prior_minus_avg, prev3_claimed_amount, claimed_amount_vs_adjuster_id_policy_id_avg, avg_claimed_amount_avg_6_prior)
) unp
