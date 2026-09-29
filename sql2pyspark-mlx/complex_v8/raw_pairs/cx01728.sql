WITH scoped AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        reported_at
    FROM claims
    WHERE approved_amount > 492.6
      AND status = 'APPROVED'
),
joined AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.reported_at,
        a.team AS adjuster_team,
        a.level AS adjuster_level,
        a.adjuster_name
    FROM scoped s
    JOIN adjusters a ON a.id = s.adjuster_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(adjuster_level) OVER (ORDER BY adjuster_level ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_adjuster_level_so_far,
        ROUND(adjuster_level / NULLIF(SUM(adjuster_level) OVER (PARTITION BY adjuster_level, adjuster_team), 0), 4) AS adjuster_level_share_of_adjuster_level_adjuster_,
        ROW_NUMBER() OVER (PARTITION BY adjuster_team, adjuster_id ORDER BY reported_at, id) AS reported_at_seq,
        LEAD(adjuster_level, 3, 0) OVER (PARTITION BY policy_id ORDER BY reported_at DESC NULLS FIRST, id) AS next3_adjuster_level,
        LAG(adjuster_level, 3) OVER w_policy_id_by_reported_at AS prev3_adjuster_level,
        NTILE(3) OVER w_adjuster_team_adjuster_id_by AS reported_at_tercile,
        ROUND(AVG(adjuster_level) OVER (ORDER BY adjuster_level ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS adjuster_level_avg_2_prior,
        ROUND(PERCENT_RANK() OVER (ORDER BY adjuster_level ASC), 3) AS adjuster_level_percent_rank
    FROM joined
    WINDOW w_policy_id_by_reported_at AS (PARTITION BY policy_id ORDER BY reported_at DESC NULLS FIRST, id), w_adjuster_team_adjuster_id_by AS (PARTITION BY adjuster_team, adjuster_id ORDER BY reported_at, id)
),
top_per_group AS (
    -- Top 3 per group
    SELECT
        id,
        policy_id,
        adjuster_id,
        reported_at,
        adjuster_team,
        adjuster_level,
        adjuster_name,
        min_adjuster_level_so_far,
        adjuster_level_share_of_adjuster_level_adjuster_,
        next3_adjuster_level,
        prev3_adjuster_level,
        reported_at_tercile,
        adjuster_level_avg_2_prior,
        adjuster_level_percent_rank
    FROM with_windows
    WHERE reported_at_seq <= 3
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        adjuster_id,
        COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids,
        MIN(reported_at) AS first_reported_at,
        SUM(adjuster_level_percent_rank) FILTER (WHERE UPPER(TRIM(adjuster_name)) LIKE 'B%') AS adjuster_level_percent_rank_upper_trim_adjuster_,
        COUNT(*) AS cnt,
        ROUND(SUM(adjuster_level) / NULLIF(SUM(min_adjuster_level_so_far), 0), 3) AS adjuster_level_to_min_adjuster_level_so_far_rati
    FROM top_per_group
    GROUP BY adjuster_id
    HAVING MAX(adjuster_level_share_of_adjuster_level_adjuster_) > 0.1
),
with_windows_2 AS (
    -- Previous / next values per key
    SELECT
        adjuster_id,
        distinct_adjuster_ids,
        first_reported_at,
        adjuster_level_percent_rank_upper_trim_adjuster_,
        cnt,
        adjuster_level_to_min_adjuster_level_so_far_rati,
        LAST_VALUE(cnt) IGNORE NULLS OVER (PARTITION BY adjuster_id, cnt ORDER BY adjuster_level_to_min_adjuster_level_so_far_rati ASC, adjuster_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cnt,
        MIN(cnt) OVER (PARTITION BY adjuster_id, cnt ORDER BY adjuster_level_to_min_adjuster_level_so_far_rati ASC, adjuster_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_cnt_so_far,
        NTH_VALUE(cnt, 2) OVER (PARTITION BY adjuster_id, cnt ORDER BY adjuster_level_to_min_adjuster_level_so_far_rati ASC, adjuster_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_cnt,
        ROUND(distinct_adjuster_ids / NULLIF(SUM(distinct_adjuster_ids) OVER (PARTITION BY adjuster_id), 0), 4) AS distinct_adjuster_ids_share_of_adjuster_id
    FROM stats
)
SELECT
    adjuster_id,
    distinct_adjuster_ids,
    cnt,
    adjuster_level_to_min_adjuster_level_so_far_rati,
    last_known_cnt,
    min_cnt_so_far,
    second_cnt,
    COALESCE(cnt, 0) AS cnt_filled
FROM with_windows_2
WHERE distinct_adjuster_ids_share_of_adjuster_id < 0.6
ORDER BY cnt_filled, adjuster_id
LIMIT 20
