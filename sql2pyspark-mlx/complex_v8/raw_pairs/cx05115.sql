WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE (claimed_amount >= 17759.3 OR claim_date BETWEEN DATE '2024-02-05' AND DATE '2025-10-30')
),
enriched AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.status,
        a.team AS adjuster_team,
        a.adjuster_name,
        a.level AS adjuster_level
    FROM valid s
    JOIN adjusters a ON a.id = s.adjuster_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        approved_amount,
        status,
        adjuster_team,
        adjuster_name,
        adjuster_level,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY adjuster_team, adjuster_level)) AS distinct_policy_id_in_adjuster_team_adjuster_lev,
        NTILE(3) OVER (ORDER BY adjuster_level DESC, id) AS adjuster_level_tercile,
        SUM(adjuster_level) OVER w_adjuster_team_adjuster_level AS adjuster_team_adjuster_level_sum_adjuster_level,
        FIRST_VALUE(approved_amount) IGNORE NULLS OVER (ORDER BY approved_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_approved_amount,
        SUM(approved_amount) OVER (ORDER BY approved_amount ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS approved_amount_sum_7,
        ROUND(adjuster_level / NULLIF(SUM(adjuster_level) OVER w_status_adjuster_team, 0), 4) AS adjuster_level_share_of_status_adjuster_team,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY status, adjuster_team)) AS distinct_policy_id_in_status_adjuster_team,
        ROUND(CUME_DIST() OVER w_policy_id_by_approved_amount, 3) AS approved_amount_cume_dist,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY adjuster_level)) AS distinct_policy_id_in_adjuster_level,
        adjuster_level - LAG(adjuster_level) OVER (PARTITION BY policy_id ORDER BY approved_amount, id) AS adjuster_level_change
    FROM enriched
    WINDOW w_adjuster_team_adjuster_level AS (PARTITION BY adjuster_team, adjuster_level), w_status_adjuster_team AS (PARTITION BY status, adjuster_team), w_policy_id_by_approved_amount AS (PARTITION BY policy_id ORDER BY approved_amount, id)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        approved_amount,
        status,
        adjuster_team,
        adjuster_name,
        adjuster_level,
        distinct_policy_id_in_adjuster_team_adjuster_lev,
        adjuster_level_tercile,
        adjuster_team_adjuster_level_sum_adjuster_level,
        first_known_approved_amount,
        adjuster_level_share_of_status_adjuster_team,
        distinct_policy_id_in_adjuster_level,
        LEAD(adjuster_level_change) OVER w_all_by_adjuster_level_tercil AS next_adjuster_level_change,
        ROW_NUMBER() OVER w_all_by_adjuster_level_tercil AS adjuster_level_tercile_seq,
        MAX(approved_amount) OVER (ORDER BY adjuster_level_change, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS approved_amount_max_1_prior
    FROM with_windows
    WINDOW w_all_by_adjuster_level_tercil AS (ORDER BY adjuster_level_tercile, id)
),
summary AS (
    -- Group-level totals
    SELECT
        distinct_policy_id_in_adjuster_team_adjuster_lev,
        COUNT(*) AS row_count,
        ROUND(AVG(approved_amount), 2) AS avg_approved_amount,
        COUNT(*) FILTER (WHERE status NOT IN ('IN_REVIEW', 'OPEN')) AS n_status_not_in_in_review_open,
        ROUND(AVG(distinct_policy_id_in_adjuster_level), 2) AS avg_distinct_policy_id_in_adjuster_level,
        COUNT(*) FILTER (WHERE UPPER(TRIM(adjuster_name)) LIKE 'A%') AS n_upper_trim_adjuster_name_lik,
        COUNT(DISTINCT id) AS distinct_ids
    FROM ranked
    WHERE adjuster_level_tercile < 6
    GROUP BY distinct_policy_id_in_adjuster_team_adjuster_lev
)
-- Final projection
SELECT
    distinct_policy_id_in_adjuster_team_adjuster_lev,
    row_count,
    avg_approved_amount,
    n_status_not_in_in_review_open,
    avg_distinct_policy_id_in_adjuster_level,
    n_upper_trim_adjuster_name_lik,
    distinct_ids,
    ABS(row_count - 8) AS row_count_distance
FROM summary
ORDER BY avg_distinct_policy_id_in_adjuster_level NULLS LAST
