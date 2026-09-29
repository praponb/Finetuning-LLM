WITH prepared AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at,
        QUARTER(played_at) AS played_at_quarter,
        CASE WHEN device IN ('speaker', 'tv') THEN 'group_a' WHEN device = 'desktop' THEN 'group_b' ELSE 'other' END AS device_group
    FROM plays
    WHERE played_at < TIMESTAMP '2025-02-12 09:00:00'
      AND source IS NOT NULL
),
rollup_base AS (
    -- Summarise per group
    SELECT
        YEAR(played_at) AS played_at_year,
        COUNT(*) FILTER (WHERE is_skipped = TRUE) AS n_is_skipped_true,
        COUNT(*) AS row_count,
        SUM(played_at_quarter) AS total_played_at_quarter,
        ARRAY_SORT(COLLECT_SET(source)) AS source_list,
        COUNT(DISTINCT track_id) AS distinct_track_ids,
        SUM(CASE WHEN source = 'album' THEN days_since_played_at ELSE 0 END) AS days_since_played_at_source_album
    FROM prepared
    WHERE ms_played > 160264
    GROUP BY YEAR(played_at)
    HAVING SUM(ms_played) > 151112
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        distinct_track_ids + n_is_skipped_true AS distinct_track_ids_plus_n_is_skipped_true
    FROM rollup_base
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_distinct_track_ids_by_distin, 3) AS distinct_track_ids_plus_n_is_skipped_true_cume_d,
        NTH_VALUE(total_played_at_quarter, 2) OVER (PARTITION BY row_count ORDER BY distinct_track_ids ASC NULLS LAST, played_at_year ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_total_played_at_quarter,
        DENSE_RANK() OVER (PARTITION BY distinct_track_ids ORDER BY distinct_track_ids_plus_n_is_skipped_true DESC, played_at_year) AS distinct_track_ids_plus_n_is_skipped_true_dense_,
        ROUND(distinct_track_ids - AVG(distinct_track_ids) OVER w_row_count, 2) AS distinct_track_ids_vs_row_count_avg,
        NTH_VALUE(n_is_skipped_true, 2) OVER (PARTITION BY distinct_track_ids ORDER BY distinct_track_ids_plus_n_is_skipped_true DESC, played_at_year ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_n_is_skipped_true
    FROM derived
    WINDOW w_distinct_track_ids_by_distin AS (PARTITION BY distinct_track_ids ORDER BY distinct_track_ids_plus_n_is_skipped_true DESC, played_at_year), w_row_count AS (PARTITION BY row_count)
)
-- Final projection
SELECT
    played_at_year,
    n_is_skipped_true,
    row_count,
    source_list,
    distinct_track_ids,
    days_since_played_at_source_album,
    distinct_track_ids_plus_n_is_skipped_true_cume_d,
    second_total_played_at_quarter,
    distinct_track_ids_plus_n_is_skipped_true_dense_,
    second_n_is_skipped_true
FROM ranked
ORDER BY distinct_track_ids, played_at_year DESC
