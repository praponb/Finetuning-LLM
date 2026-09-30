WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM claims
    WHERE adjuster_id IS NOT NULL
      AND claim_type IS NOT NULL
),
combined_details_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        p.premium AS policy_premium,
        p.status AS policy_status,
        p.end_date AS policy_end_date
    FROM prepared s
    LEFT JOIN policies p ON p.id = s.policy_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        status,
        policy_premium,
        policy_status,
        policy_end_date,
        RANK() OVER (PARTITION BY status, TO_DATE(reported_at) ORDER BY approved_amount DESC NULLS FIRST, id) AS approved_amount_rank,
        MIN(policy_premium) OVER (PARTITION BY status, policy_status) AS status_policy_status_min_policy_premium,
        MIN(policy_premium) OVER (PARTITION BY status) AS status_min_policy_premium,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY status, policy_status), 0), 4) AS claimed_amount_share_of_status_policy_status,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY status)) AS distinct_policy_id_in_status,
        NTH_VALUE(claimed_amount, 2) OVER (ORDER BY policy_end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claimed_amount,
        policy_premium - LAG(policy_premium) OVER (PARTITION BY YEAR(policy_end_date), policy_id ORDER BY policy_end_date, id) AS policy_premium_change,
        LEAD(policy_premium) OVER (PARTITION BY status, TO_DATE(reported_at) ORDER BY approved_amount DESC NULLS FIRST, id) AS next_policy_premium,
        LAST_VALUE(reported_at) IGNORE NULLS OVER (PARTITION BY status ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_reported_at,
        SIZE(COLLECT_SET(adjuster_id) OVER w_status) AS distinct_adjuster_id_in_status,
        NTH_VALUE(approved_amount, 2) OVER (PARTITION BY status ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        LAG(claimed_amount, 2) OVER (ORDER BY policy_end_date, id) AS prev2_claimed_amount
    FROM combined_details_policy
    WINDOW w_status AS (PARTITION BY status)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        policy_premium,
        approved_amount_rank,
        status_policy_status_min_policy_premium,
        claimed_amount_share_of_status_policy_status,
        policy_premium_change,
        next_policy_premium,
        last_known_reported_at,
        distinct_adjuster_id_in_status,
        second_approved_amount,
        prev2_claimed_amount,
        SIZE(COLLECT_SET(policy_status) OVER w_policy_id_distinct_policy_id) AS distinct_policy_status_in_policy_id_distinct_pol,
        COUNT(*) OVER (ORDER BY approved_amount NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(AVG(next_policy_premium) OVER (PARTITION BY policy_id, distinct_policy_id_in_status), 2) AS policy_id_distinct_policy_id_in_status_avg_next_,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY policy_id, distinct_policy_id_in_status)) AS distinct_status_in_policy_id_distinct_policy_id_
    FROM windowed
    WINDOW w_policy_id_distinct_policy_id AS (PARTITION BY policy_id, distinct_policy_id_in_status)
)
SELECT
    id,
    policy_id,
    adjuster_id,
    policy_premium,
    approved_amount_rank,
    status_policy_status_min_policy_premium,
    claimed_amount_share_of_status_policy_status,
    policy_premium_change,
    next_policy_premium,
    last_known_reported_at,
    distinct_adjuster_id_in_status,
    second_approved_amount,
    prev2_claimed_amount,
    distinct_policy_status_in_policy_id_distinct_pol,
    running_count,
    policy_id_distinct_policy_id_in_status_avg_next_,
    distinct_status_in_policy_id_distinct_policy_id_
FROM with_windows
WHERE second_approved_amount < 26237.1
