WITH recent AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        reported_at
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 10 AND 13
),
screened AS (
    -- Keep rows whose adjuster matches
    SELECT s.*
    FROM recent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM adjusters a
            WHERE a.id = s.adjuster_id
              AND a.level >= 1
        )
),
screened_2 AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM screened s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.claimed_amount < 43500.4
        )
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY policy_id, adjuster_id) AS rows_in_policy_id_adjuster_id,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY policy_id, adjuster_id), 0), 4) AS approved_amount_share_of_policy_id_adjuster_id,
        RANK() OVER (ORDER BY approved_amount DESC, id) AS approved_amount_rank,
        NTH_VALUE(claimed_amount, 2) OVER (ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claimed_amount,
        LEAD(claimed_amount, 2) OVER (ORDER BY approved_amount DESC, id) AS next2_claimed_amount
    FROM screened_2
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY approved_amount), 3) AS approved_amount_cume_dist,
        ROUND(AVG(approved_amount_rank) OVER w_adjuster_id_approved_amount, 2) AS adjuster_id_approved_amount_rank_avg_approved_am,
        ROUND(next2_claimed_amount - AVG(next2_claimed_amount) OVER (PARTITION BY adjuster_id, approved_amount_rank), 2) AS next2_claimed_amount_vs_adjuster_id_approved_amo,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER w_adjuster_id_approved_amount, 0), 4) AS approved_amount_share_of_adjuster_id_approved_am
    FROM ranked
    WINDOW w_adjuster_id_approved_amount AS (PARTITION BY adjuster_id, approved_amount_rank)
),
policy_id_stats AS (
    -- Max next2_claimed_amount per policy_id
    SELECT policy_id, MAX(next2_claimed_amount) AS max_next2_claimed_amount
    FROM metrics
    GROUP BY policy_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_next2_claimed_amount,
        ROUND(s.next2_claimed_amount - g.max_next2_claimed_amount, 2) AS next2_claimed_amount_minus_max
    FROM metrics s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.next2_claimed_amount = g.max_next2_claimed_amount
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(second_claimed_amount / 10000) * 10000 AS second_claimed_amount_bucket,
        CASE WHEN claimed_amount >= 29191.2 THEN 'HIGH' WHEN claimed_amount >= 12010.5 THEN 'MEDIUM' ELSE 'LOW' END AS claimed_amount_tier,
        DATEDIFF(DATE '2025-06-30', reported_at) AS days_since_reported_at
    FROM compared
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claimed_amount,
    approved_amount,
    reported_at,
    rows_in_policy_id_adjuster_id,
    approved_amount_share_of_policy_id_adjuster_id,
    approved_amount_rank,
    second_claimed_amount,
    next2_claimed_amount,
    approved_amount_cume_dist,
    adjuster_id_approved_amount_rank_avg_approved_am,
    next2_claimed_amount_vs_adjuster_id_approved_amo,
    approved_amount_share_of_adjuster_id_approved_am,
    next2_claimed_amount_minus_max,
    second_claimed_amount_bucket,
    claimed_amount_tier,
    days_since_reported_at,
    CASE WHEN days_since_reported_at >= 560 THEN 'HIGH' WHEN days_since_reported_at >= 140 THEN 'MEDIUM' ELSE 'LOW' END AS days_since_reported_at_tier,
    ABS(next2_claimed_amount - 20843.1) AS next2_claimed_amount_distance
FROM annotated
ORDER BY adjuster_id_approved_amount_rank_avg_approved_am, next2_claimed_amount_vs_adjuster_id_approved_amo
