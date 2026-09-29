WITH prepared_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        reported_at + INTERVAL 30 MINUTES AS reported_at_plus_30m
    FROM claims
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        adjuster_id,
        reported_at_plus_30m,
        ROUND(CUME_DIST() OVER (PARTITION BY status ORDER BY claim_date ASC), 3) AS claim_date_cume_dist,
        NTH_VALUE(reported_at_plus_30m, 2) OVER (PARTITION BY status ORDER BY claim_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at_plus_30m,
        FIRST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY status ORDER BY claim_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claim_date,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(claim_date), adjuster_id ORDER BY reported_at_plus_30m), 3) AS reported_at_plus_30m_cume_dist,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY MONTH(claim_date), adjuster_id ORDER BY reported_at_plus_30m, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        NTH_VALUE(reported_at_plus_30m, 2) OVER (PARTITION BY policy_id ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at_plus_30m_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status ORDER BY claim_date ASC), 3) AS claim_date_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY claim_date ASC, id) AS rn,
        ROUND((UNIX_TIMESTAMP(reported_at_plus_30m) - UNIX_TIMESTAMP(LAG(reported_at_plus_30m) OVER (PARTITION BY MONTH(claim_date), adjuster_id ORDER BY reported_at_plus_30m, id))) / 60, 1) AS minutes_since_prev_reported_at_plus_30m
    FROM prepared_claims
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        reported_at_plus_30m,
        claim_date_cume_dist,
        second_reported_at_plus_30m,
        first_known_claim_date,
        second_claim_date,
        second_reported_at_plus_30m_2,
        claim_date_percent_rank,
        rn,
        minutes_since_prev_reported_at_plus_30m,
        ROUND(rn - AVG(rn) OVER w_second_reported_at_plus_30m, 2) AS rn_vs_second_reported_at_plus_30m_to_date_second,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY TO_DATE(second_reported_at_plus_30m), YEAR(second_reported_at_plus_30m_2))) AS distinct_policy_id_in_second_reported_at_plus_30,
        SUM(claim_date_cume_dist) OVER (PARTITION BY TO_DATE(second_reported_at_plus_30m_2), rn ORDER BY second_claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claim_date_cume_dist,
        LEAD(claim_date_cume_dist, 1, 0) OVER w_policy_id_by_second_reported AS next_claim_date_cume_dist,
        ROUND((UNIX_TIMESTAMP(second_reported_at_plus_30m) - UNIX_TIMESTAMP(LAG(second_reported_at_plus_30m) OVER w_policy_id_by_second_reported)) / 60, 1) AS minutes_since_prev_second_reported_at_plus_30m
    FROM windowed
    WINDOW w_second_reported_at_plus_30m AS (PARTITION BY TO_DATE(second_reported_at_plus_30m), YEAR(second_reported_at_plus_30m_2)), w_policy_id_by_second_reported AS (PARTITION BY policy_id ORDER BY second_reported_at_plus_30m, id)
)
SELECT
    id,
    policy_id,
    adjuster_id,
    reported_at_plus_30m,
    claim_date_cume_dist,
    second_reported_at_plus_30m,
    first_known_claim_date,
    second_claim_date,
    second_reported_at_plus_30m_2,
    claim_date_percent_rank,
    rn,
    minutes_since_prev_reported_at_plus_30m,
    rn_vs_second_reported_at_plus_30m_to_date_second,
    distinct_policy_id_in_second_reported_at_plus_30,
    running_claim_date_cume_dist,
    next_claim_date_cume_dist,
    minutes_since_prev_second_reported_at_plus_30m
FROM with_windows
ORDER BY second_claim_date DESC
