WITH recent AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        status
    FROM claims
),
with_details AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.status,
        a.level AS adjuster_level,
        a.adjuster_name
    FROM recent s
    JOIN adjusters a ON a.id = s.adjuster_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        approved_amount,
        status,
        adjuster_level,
        ROW_NUMBER() OVER (PARTITION BY adjuster_id ORDER BY adjuster_level NULLS LAST, id) AS rn,
        COUNT(*) OVER (PARTITION BY status, adjuster_id) AS rows_in_status_adjuster_id,
        NTILE(3) OVER (ORDER BY approved_amount ASC NULLS LAST, id) AS approved_amount_tercile,
        SUM(approved_amount) OVER (PARTITION BY adjuster_id, status ORDER BY adjuster_level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_approved_amount,
        MAX(adjuster_level) OVER (ORDER BY adjuster_level DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_adjuster_level_so_far,
        ROUND(AVG(approved_amount) OVER (PARTITION BY status), 2) AS status_avg_approved_amount,
        LAG(approved_amount) OVER (ORDER BY adjuster_level DESC, id) AS prev_approved_amount,
        approved_amount - LAG(approved_amount) OVER (ORDER BY approved_amount ASC NULLS LAST, id) AS approved_amount_change,
        NTH_VALUE(adjuster_level, 2) OVER (PARTITION BY adjuster_id, policy_id ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_adjuster_level,
        DENSE_RANK() OVER (ORDER BY adjuster_level DESC) AS adjuster_level_dense_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY approved_amount ASC NULLS LAST, id), 3) AS approved_amount_percent_rank
    FROM with_details
),
leaders AS (
    -- Top 5 per group
    SELECT *
    FROM ranked
    WHERE rn <= 5
      AND approved_amount_tercile >= 3
),
ranked_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        approved_amount,
        status,
        adjuster_level,
        approved_amount_tercile,
        running_approved_amount,
        max_adjuster_level_so_far,
        status_avg_approved_amount,
        approved_amount_change,
        second_adjuster_level,
        adjuster_level_dense_rank,
        ROUND(PERCENT_RANK() OVER w_all_by_running_approved_amou, 3) AS running_approved_amount_percent_rank,
        adjuster_level - LAG(adjuster_level) OVER (PARTITION BY approved_amount_tercile, policy_id ORDER BY adjuster_level_dense_rank, id) AS adjuster_level_change,
        ROUND(adjuster_level - AVG(adjuster_level) OVER (PARTITION BY second_adjuster_level), 2) AS adjuster_level_vs_second_adjuster_level_avg,
        LAG(rows_in_status_adjuster_id) OVER (ORDER BY prev_approved_amount DESC, id) AS prev_rows_in_status_adjuster_id
    FROM leaders
    WINDOW w_all_by_running_approved_amou AS (ORDER BY running_approved_amount ASC NULLS LAST)
)
SELECT
    id,
    policy_id,
    approved_amount,
    status,
    adjuster_level,
    approved_amount_tercile,
    running_approved_amount,
    max_adjuster_level_so_far,
    status_avg_approved_amount,
    approved_amount_change,
    second_adjuster_level,
    adjuster_level_dense_rank,
    adjuster_level_change,
    adjuster_level_vs_second_adjuster_level_avg,
    prev_rows_in_status_adjuster_id,
    UPPER(TRIM(status)) AS status_clean
FROM ranked_2
