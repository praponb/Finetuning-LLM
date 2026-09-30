WITH recent AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status
    FROM claims
    WHERE reported_at >= TIMESTAMP '2025-01-25 13:00:00'
      AND claim_type IN ('medical', 'theft', 'delay')
),
with_parent AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.status,
        p.end_date AS policy_end_date,
        p.start_date AS policy_start_date
    FROM recent s
    LEFT JOIN policies p ON p.id = s.policy_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        policy_end_date,
        policy_start_date,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY status, YEAR(claim_date)), 0), 4) AS claimed_amount_share_of_status_claim_date_year,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY MONTH(policy_start_date), status ORDER BY policy_start_date DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_avg_7,
        NTH_VALUE(policy_end_date, 2) OVER (ORDER BY policy_start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_policy_end_date,
        LAST_VALUE(claimed_amount) IGNORE NULLS OVER (ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claimed_amount,
        MIN(claimed_amount) OVER (PARTITION BY policy_id) AS policy_id_min_claimed_amount,
        claimed_amount - LAG(claimed_amount) OVER (PARTITION BY policy_id, claim_type ORDER BY policy_start_date, id) AS claimed_amount_change,
        DENSE_RANK() OVER (PARTITION BY YEAR(policy_end_date) ORDER BY policy_start_date DESC) AS policy_start_date_dense_rank,
        LEAD(claim_date, 3) OVER w_policy_start_date_month_stat_2 AS next3_claim_date,
        LAG(policy_end_date, 3) OVER (PARTITION BY YEAR(policy_end_date) ORDER BY policy_start_date DESC, id) AS prev3_policy_end_date,
        LAST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY YEAR(policy_end_date) ORDER BY policy_start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claimed_amount_2,
        LAST_VALUE(claim_date) IGNORE NULLS OVER (ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date,
        MAX(claimed_amount) OVER (ORDER BY policy_start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_claimed_amount_so_far
    FROM with_parent
    WINDOW w_policy_start_date_month_stat_2 AS (PARTITION BY MONTH(policy_start_date), status ORDER BY policy_start_date DESC, id)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        claimed_amount_avg_7,
        second_policy_end_date,
        last_known_claimed_amount,
        policy_id_min_claimed_amount,
        policy_start_date_dense_rank,
        last_known_claimed_amount_2,
        last_known_claim_date,
        max_claimed_amount_so_far,
        ROUND(CUME_DIST() OVER w_status_by_prev3_policy_end_d, 3) AS prev3_policy_end_date_cume_dist,
        NTH_VALUE(prev3_policy_end_date, 2) OVER (PARTITION BY status, MONTH(claim_date) ORDER BY claimed_amount_change DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prev3_policy_end_date,
        SUM(max_claimed_amount_so_far) OVER w_policy_start_date_year AS policy_start_date_year_sum_max_claimed_amount_so,
        ROW_NUMBER() OVER (PARTITION BY status, MONTH(claim_date) ORDER BY claimed_amount_change DESC NULLS FIRST, id) AS rn,
        ROUND(AVG(policy_id_min_claimed_amount) OVER (PARTITION BY status ORDER BY prev3_policy_end_date DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS policy_id_min_claimed_amount_avg_3_prior,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY YEAR(policy_start_date))) AS distinct_adjuster_id_in_policy_start_date_year
    FROM with_trends
    WINDOW w_status_by_prev3_policy_end_d AS (PARTITION BY status ORDER BY prev3_policy_end_date DESC, id), w_policy_start_date_year AS (PARTITION BY YEAR(policy_start_date))
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claimed_amount,
    status,
    claimed_amount_avg_7,
    second_policy_end_date,
    last_known_claimed_amount,
    last_known_claimed_amount_2,
    last_known_claim_date,
    max_claimed_amount_so_far,
    prev3_policy_end_date_cume_dist,
    second_prev3_policy_end_date,
    rn,
    policy_id_min_claimed_amount_avg_3_prior,
    distinct_adjuster_id_in_policy_start_date_year
FROM windowed
ORDER BY prev3_policy_end_date_cume_dist DESC
