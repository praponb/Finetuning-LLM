WITH cleaned AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        UPPER(TRIM(status)) AS status_clean
    FROM claims
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.status_clean,
        p.end_date AS policy_end_date,
        a.level AS adjuster_level,
        a.adjuster_name
    FROM cleaned s
    LEFT JOIN policies p ON p.id = s.policy_id
    JOIN adjusters a ON a.id = s.adjuster_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        claimed_amount,
        approved_amount,
        reported_at,
        policy_end_date,
        adjuster_level,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY status, MONTH(reported_at) ORDER BY adjuster_level DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS claimed_amount_avg_6_prior,
        DENSE_RANK() OVER (PARTITION BY adjuster_id ORDER BY claimed_amount ASC) AS claimed_amount_dense_rank,
        NTILE(3) OVER (PARTITION BY adjuster_id ORDER BY claimed_amount ASC, id) AS claimed_amount_tercile,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY adjuster_level)) AS distinct_adjuster_id_in_adjuster_level,
        LEAD(claimed_amount) OVER (ORDER BY policy_end_date, id) AS next_claimed_amount,
        MIN(adjuster_level) OVER (PARTITION BY adjuster_id, MONTH(policy_end_date)) AS adjuster_id_policy_end_date_month_min_adjuster_l,
        SUM(approved_amount) OVER w_adjuster_id_policy_end_date AS adjuster_id_policy_end_date_month_sum_approved_a
    FROM joined
    WINDOW w_adjuster_id_policy_end_date AS (PARTITION BY adjuster_id, MONTH(policy_end_date))
),
first_rows AS (
    -- First row per group
    SELECT
        id,
        claimed_amount,
        approved_amount,
        reported_at,
        policy_end_date,
        adjuster_level,
        claimed_amount_avg_6_prior,
        claimed_amount_tercile,
        distinct_adjuster_id_in_adjuster_level,
        next_claimed_amount,
        adjuster_id_policy_end_date_month_min_adjuster_l,
        adjuster_id_policy_end_date_month_sum_approved_a
    FROM with_trends
    WHERE claimed_amount_dense_rank = 1
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY distinct_adjuster_id_in_adjuster_level) AS rows_in_distinct_adjuster_id_in_adjuster_level,
        ROUND(claimed_amount_avg_6_prior / NULLIF(SUM(claimed_amount_avg_6_prior) OVER (PARTITION BY distinct_adjuster_id_in_adjuster_level), 0), 4) AS claimed_amount_avg_6_prior_share_of_distinct_adj,
        claimed_amount - LAG(claimed_amount) OVER (PARTITION BY claimed_amount_tercile, adjuster_level ORDER BY claimed_amount DESC, id) AS claimed_amount_change,
        FIRST_VALUE(adjuster_level) OVER (PARTITION BY distinct_adjuster_id_in_adjuster_level, adjuster_level ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_adjuster_level
    FROM first_rows
)
SELECT
    id,
    claimed_amount,
    approved_amount,
    reported_at,
    claimed_amount_avg_6_prior,
    claimed_amount_tercile,
    distinct_adjuster_id_in_adjuster_level,
    adjuster_id_policy_end_date_month_min_adjuster_l,
    adjuster_id_policy_end_date_month_sum_approved_a,
    rows_in_distinct_adjuster_id_in_adjuster_level,
    claimed_amount_avg_6_prior_share_of_distinct_adj,
    claimed_amount_change,
    first_adjuster_level,
    reported_at + INTERVAL 1 DAYS AS reported_at_plus_1d
FROM windowed
ORDER BY claimed_amount
