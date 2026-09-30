WITH valid AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        status,
        reported_at
    FROM claims
    WHERE (status = 'PAID' OR adjuster_id IS NOT NULL)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        claim_type,
        status,
        NTH_VALUE(reported_at, 2) OVER (PARTITION BY status ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at,
        ROW_NUMBER() OVER (ORDER BY reported_at, id) AS rn,
        FIRST_VALUE(reported_at) IGNORE NULLS OVER (PARTITION BY status ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_reported_at,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER (PARTITION BY status ORDER BY reported_at, id))) / 60, 1) AS minutes_since_prev_reported_at,
        ROUND(CUME_DIST() OVER w_status_by_reported_at, 3) AS reported_at_cume_dist,
        LEAD(reported_at, 2) OVER w_all_by_reported_at AS next2_reported_at
    FROM valid
    WINDOW w_status_by_reported_at AS (PARTITION BY status ORDER BY reported_at, id), w_all_by_reported_at AS (ORDER BY reported_at, id)
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.claim_type,
        s.second_reported_at,
        s.rn,
        s.first_known_reported_at,
        s.reported_at_cume_dist,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.claim_date
    FROM with_windows s
    JOIN claims c ON c.policy_id = s.policy_id AND c.claim_date < DATE '2025-05-26'
),
claim_type_stats AS (
    -- Avg reported_at_cume_dist per claim_type
    SELECT claim_type, AVG(reported_at_cume_dist) AS avg_reported_at_cume_dist
    FROM exploded_detail_claims
    GROUP BY claim_type
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_reported_at_cume_dist,
        ROUND(s.reported_at_cume_dist - g.avg_reported_at_cume_dist, 2) AS reported_at_cume_dist_minus_avg
    FROM exploded_detail_claims s
    JOIN claim_type_stats g ON g.claim_type = s.claim_type
    WHERE s.reported_at_cume_dist > g.avg_reported_at_cume_dist
),
grouped AS (
    -- Summarise per group
    SELECT
        rn,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
        MIN(claim_claimed_amount) AS min_claim_claimed_amount,
        SUM(reported_at_cume_dist_minus_avg) AS total_reported_at_cume_dist_minus_avg,
        SUM(claim_claimed_amount) AS total_claim_claimed_amount,
        ROUND(SUM(avg_reported_at_cume_dist), 2) AS total_avg_reported_at_cume_dist
    FROM outliers
    WHERE claim_id IS NOT NULL
    GROUP BY rn
    HAVING AVG(claim_claimed_amount) > 3703.0
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(rn) OVER (ORDER BY min_claim_claimed_amount NULLS LAST, rn ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS rn_avg_6_prior,
        ROUND(min_claim_claimed_amount - AVG(min_claim_claimed_amount) OVER w_rn, 2) AS min_claim_claimed_amount_vs_rn_avg
    FROM grouped
    WINDOW w_rn AS (PARTITION BY rn)
)
-- Final projection
SELECT
    rn,
    total_claim_claimed_amount,
    LEAST(min_claim_claimed_amount, rn_avg_6_prior) AS least_min_claim_claimed_amount_rn_avg_6_prior,
    rn + rn_avg_6_prior AS rn_plus_rn_avg_6_prior
FROM metrics
WHERE total_avg_reported_at_cume_dist BETWEEN 1.8 AND 4.2
ORDER BY least_min_claim_claimed_amount_rn_avg_6_prior
