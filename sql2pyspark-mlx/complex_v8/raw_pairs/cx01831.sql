WITH filtered AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        reported_at,
        DATE_TRUNC('HOUR', reported_at) AS reported_at_hour
    FROM claims
    WHERE claimed_amount < 39666.5
      AND claim_date >= DATE '2024-06-07'
),
with_details_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.*,
        a.level AS adjuster_level,
        a.adjuster_name,
        a.team AS adjuster_team
    FROM filtered s
    JOIN adjusters a ON a.id = s.adjuster_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(adjuster_level - AVG(adjuster_level) OVER (PARTITION BY adjuster_id, adjuster_team), 2) AS adjuster_level_vs_adjuster_id_adjuster_team_avg,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY adjuster_id, TO_DATE(reported_at) ORDER BY reported_at_hour DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        ROUND(AVG(adjuster_level) OVER (ORDER BY claim_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS adjuster_level_avg_6_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY adjuster_id ORDER BY adjuster_level), 3) AS adjuster_level_percent_rank,
        ROUND(AVG(adjuster_level) OVER (PARTITION BY policy_id ORDER BY reported_at DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS adjuster_level_avg_3_prior,
        NTILE(10) OVER (PARTITION BY policy_id ORDER BY reported_at DESC, id) AS reported_at_decile,
        COUNT(*) OVER w_adjuster_id_adjuster_team AS rows_in_adjuster_id_adjuster_team,
        LAG(reported_at, 2) OVER (PARTITION BY adjuster_id, TO_DATE(reported_at) ORDER BY reported_at_hour DESC NULLS FIRST, id) AS prev2_reported_at,
        DENSE_RANK() OVER (ORDER BY claim_date) AS claim_date_dense_rank
    FROM with_details_adjuster
    WINDOW w_adjuster_id_adjuster_team AS (PARTITION BY adjuster_id, adjuster_team)
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM with_windows
    WHERE claim_date_dense_rank = 1
      AND adjuster_team NOT IN ('Travel', 'Property')
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        reported_at,
        reported_at_hour,
        adjuster_level,
        adjuster_name,
        adjuster_team,
        adjuster_level_vs_adjuster_id_adjuster_team_avg,
        second_claim_date,
        adjuster_level_avg_6_prior,
        adjuster_level_percent_rank,
        adjuster_level_avg_3_prior,
        reported_at_decile,
        rows_in_adjuster_id_adjuster_team,
        RANK() OVER (PARTITION BY adjuster_team, YEAR(claim_date) ORDER BY adjuster_level_vs_adjuster_id_adjuster_team_avg ASC) AS adjuster_level_vs_adjuster_id_adjuster_team_avg_,
        SIZE(COLLECT_SET(adjuster_id) OVER w_reported_at_decile_status) AS distinct_adjuster_id_in_reported_at_decile_statu,
        ROUND(adjuster_level_percent_rank - AVG(adjuster_level_percent_rank) OVER (PARTITION BY reported_at_decile, status), 2) AS adjuster_level_percent_rank_vs_reported_at_decil,
        COUNT(*) OVER (PARTITION BY reported_at_decile, status) AS rows_in_reported_at_decile_status,
        ROUND(AVG(adjuster_level_avg_3_prior) OVER (PARTITION BY reported_at_decile, status), 2) AS reported_at_decile_status_avg_adjuster_level_avg,
        ROUND(CUME_DIST() OVER w_all_by_rows_in_adjuster_id_a, 3) AS rows_in_adjuster_id_adjuster_team_cume_dist,
        DATEDIFF(claim_date, LAG(claim_date) OVER (ORDER BY rows_in_adjuster_id_adjuster_team DESC NULLS FIRST, id)) AS days_since_prev_claim_date
    FROM top_per_group
    WINDOW w_reported_at_decile_status AS (PARTITION BY reported_at_decile, status), w_all_by_rows_in_adjuster_id_a AS (ORDER BY rows_in_adjuster_id_adjuster_team DESC NULLS FIRST, id)
),
per_group AS (
    -- Group-level totals
    SELECT
        adjuster_level_vs_adjuster_id_adjuster_team_avg_,
        adjuster_level,
        adjuster_team,
        SUM(adjuster_level_vs_adjuster_id_adjuster_team_avg_) AS total_adjuster_level_vs_adjuster_id_adjuster_tea,
        ARRAY_SORT(COLLECT_SET(adjuster_name)) AS adjuster_name_list,
        ROUND(SUM(distinct_adjuster_id_in_reported_at_decile_statu) / NULLIF(SUM(rows_in_reported_at_decile_status), 0), 3) AS distinct_adjuster_id_in_reported_at_decile_statu_2
    FROM ranked
    GROUP BY adjuster_level_vs_adjuster_id_adjuster_team_avg_, adjuster_level, adjuster_team
    HAVING MAX(rows_in_reported_at_decile_status) > 8
)
SELECT
    adjuster_level_vs_adjuster_id_adjuster_team_avg_,
    adjuster_level,
    adjuster_team,
    adjuster_name_list,
    distinct_adjuster_id_in_reported_at_decile_statu_2
FROM per_group
ORDER BY adjuster_team DESC NULLS FIRST, distinct_adjuster_id_in_reported_at_decile_statu_2
