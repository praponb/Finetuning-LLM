WITH recent AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        SUBSTRING(TRIM(source), 1, 4) AS source_prefix,
        CASE WHEN device IN ('speaker', 'phone') THEN 'group_a' WHEN device = 'tv' THEN 'group_b' ELSE 'other' END AS device_group,
        DATE_TRUNC('MONTH', played_at) AS played_at_month
    FROM plays
    WHERE played_at < TIMESTAMP '2025-02-12 11:00:00'
      AND device NOT IN ('tv', 'car')
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY listener_id ORDER BY played_at ASC), 3) AS played_at_percent_rank,
        ROW_NUMBER() OVER w_is_skipped_by_played_at_mont AS rn,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (PARTITION BY is_skipped ORDER BY played_at_month NULLS LAST, id))) / 60, 1) AS minutes_since_prev_played_at,
        LAG(played_at) OVER w_is_skipped_by_played_at_mont AS prev_played_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY listener_id ORDER BY played_at ASC, id), 3) AS played_at_percent_rank_2,
        LEAD(played_at_month) OVER (PARTITION BY listener_id ORDER BY played_at ASC, id) AS next_played_at_month
    FROM recent
    WINDOW w_is_skipped_by_played_at_mont AS (PARTITION BY is_skipped ORDER BY played_at_month NULLS LAST, id)
),
stats AS (
    -- Group-level totals
    SELECT
        is_skipped,
        rn,
        COUNT(*) AS row_count,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids
    FROM metrics
    WHERE listener_id <= 2
    GROUP BY is_skipped, rn
    HAVING AVG(minutes_since_prev_played_at) > 1500.0
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        is_skipped,
        rn,
        row_count,
        distinct_listener_ids,
        LAG(distinct_listener_ids, 1, 0) OVER w_distinct_listener_ids_by_dis AS prev_distinct_listener_ids,
        ROUND(AVG(distinct_listener_ids) OVER (PARTITION BY distinct_listener_ids ORDER BY distinct_listener_ids, is_skipped, rn ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS distinct_listener_ids_avg_2
    FROM stats
    WINDOW w_distinct_listener_ids_by_dis AS (PARTITION BY distinct_listener_ids ORDER BY distinct_listener_ids, is_skipped, rn)
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM sequenced
    WHERE distinct_listener_ids > (SELECT AVG(distinct_listener_ids) FROM sequenced)
),
metrics_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY is_skipped) AS rows_in_is_skipped,
        ROUND(distinct_listener_ids - AVG(distinct_listener_ids) OVER (PARTITION BY is_skipped), 2) AS distinct_listener_ids_vs_is_skipped_avg,
        ROUND(AVG(row_count) OVER (PARTITION BY rn ORDER BY distinct_listener_ids_avg_2 ASC, is_skipped, rn ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS row_count_avg_4,
        SUM(row_count) OVER (PARTITION BY is_skipped) AS is_skipped_sum_row_count
    FROM without_matches
)
-- Subtotals with GROUPING SETS
SELECT
    row_count,
    is_skipped,
    rn,
    GROUPING_ID() AS gid,
    ROUND(SUM(is_skipped_sum_row_count) / NULLIF(SUM(rn), 0), 3) AS is_skipped_sum_row_count_to_rn_ratio,
    SUM(is_skipped_sum_row_count) AS total_is_skipped_sum_row_count
FROM metrics_2
GROUP BY GROUPING SETS ((row_count, is_skipped, rn), (row_count), (is_skipped))
