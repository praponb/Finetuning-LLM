WITH base_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        reported_at
    FROM claims
    WHERE adjuster_id >= 5
      AND approved_amount <= 14377.6
),
joined AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.status,
        s.reported_at,
        a.level AS adjuster_level
    FROM base_claims s
    JOIN adjusters a ON a.id = s.adjuster_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(adjuster_level, 2) OVER (PARTITION BY status, TO_DATE(reported_at) ORDER BY claim_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_adjuster_level,
        SIZE(COLLECT_SET(policy_id) OVER w_adjuster_id) AS distinct_policy_id_in_adjuster_id,
        SUM(claimed_amount) OVER (PARTITION BY adjuster_id) AS adjuster_id_sum_claimed_amount,
        ROUND(AVG(adjuster_level) OVER w_adjuster_id, 2) AS adjuster_id_avg_adjuster_level,
        NTH_VALUE(reported_at, 2) OVER (PARTITION BY status, TO_DATE(reported_at) ORDER BY claim_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at,
        RANK() OVER (PARTITION BY status, TO_DATE(reported_at) ORDER BY claim_date ASC, id) AS claim_date_rank,
        ROUND(CUME_DIST() OVER w_all_by_claimed_amount_ties, 3) AS claimed_amount_cume_dist,
        COUNT(*) OVER (PARTITION BY adjuster_id) AS rows_in_adjuster_id
    FROM joined
    WINDOW w_adjuster_id AS (PARTITION BY adjuster_id), w_all_by_claimed_amount_ties AS (ORDER BY claimed_amount DESC)
),
top_per_group AS (
    -- Top 2 per group
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        reported_at,
        adjuster_level,
        second_adjuster_level,
        distinct_policy_id_in_adjuster_id,
        adjuster_id_sum_claimed_amount,
        adjuster_id_avg_adjuster_level,
        second_reported_at,
        claimed_amount_cume_dist,
        rows_in_adjuster_id
    FROM ranked
    WHERE claim_date_rank <= 2
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        reported_at,
        adjuster_level,
        second_adjuster_level,
        adjuster_id_sum_claimed_amount,
        adjuster_id_avg_adjuster_level,
        second_reported_at,
        COUNT(*) OVER (PARTITION BY distinct_policy_id_in_adjuster_id, MONTH(reported_at)) AS rows_in_distinct_policy_id_in_adjuster_id_report,
        ROUND(AVG(rows_in_adjuster_id) OVER (PARTITION BY status ORDER BY adjuster_id_sum_claimed_amount DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS rows_in_adjuster_id_avg_2,
        LAST_VALUE(second_reported_at) IGNORE NULLS OVER (PARTITION BY status ORDER BY adjuster_id_sum_claimed_amount DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_second_reported_at,
        FIRST_VALUE(rows_in_adjuster_id) OVER (PARTITION BY status ORDER BY adjuster_id_sum_claimed_amount DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_rows_in_adjuster_id,
        ROUND(AVG(rows_in_adjuster_id) OVER (PARTITION BY status ORDER BY adjuster_id_sum_claimed_amount DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS rows_in_adjuster_id_avg_3_prior,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY adjuster_id_sum_claimed_amount DESC NULLS FIRST, id) AS rn,
        LEAD(claimed_amount_cume_dist) OVER (PARTITION BY status ORDER BY adjuster_id_sum_claimed_amount DESC NULLS FIRST, id) AS next_claimed_amount_cume_dist
    FROM top_per_group
)
-- Final projection
SELECT DISTINCT next_claimed_amount_cume_dist, first_rows_in_adjuster_id, rn
FROM with_windows
WHERE rows_in_adjuster_id_avg_3_prior >= 7.7
