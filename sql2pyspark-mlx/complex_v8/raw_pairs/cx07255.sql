WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag,
        CASE WHEN source IN ('search', 'radio') THEN 'group_a' WHEN source = 'playlist' THEN 'group_b' ELSE 'other' END AS source_group
    FROM plays
    WHERE ms_played >= 56858
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY source_group, source ORDER BY is_skipped_flag, id), 3) AS is_skipped_flag_cume_dist,
        RANK() OVER (PARTITION BY source_group, source ORDER BY is_skipped_flag) AS is_skipped_flag_rank,
        COUNT(*) OVER w_source AS rows_in_source,
        ROUND(CUME_DIST() OVER (PARTITION BY source_group, source ORDER BY is_skipped_flag), 3) AS is_skipped_flag_cume_dist_2,
        COUNT(*) OVER (PARTITION BY track_id) AS rows_in_track_id,
        ROW_NUMBER() OVER (ORDER BY is_skipped_flag DESC, id) AS seq_no,
        ROUND(is_skipped_flag - AVG(is_skipped_flag) OVER (PARTITION BY source), 2) AS is_skipped_flag_vs_source_avg,
        FIRST_VALUE(ms_played) IGNORE NULLS OVER (ORDER BY is_skipped_flag DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played,
        LAG(ms_played, 1, 0) OVER (PARTITION BY source_group, source ORDER BY is_skipped_flag, id) AS prev_ms_played,
        ROUND(is_skipped_flag - AVG(is_skipped_flag) OVER w_track_id, 2) AS is_skipped_flag_vs_track_id_avg,
        SUM(ms_played) OVER (ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played,
        SUM(ms_played) OVER (PARTITION BY source_group, source ORDER BY is_skipped_flag, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS ms_played_sum_2_prior,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY source), 0), 4) AS ms_played_share_of_source,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY track_id), 0), 4) AS ms_played_share_of_track_id
    FROM valid
    WINDOW w_source AS (PARTITION BY source), w_track_id AS (PARTITION BY track_id)
),
top_per_group AS (
    -- Top 2 per group
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        is_skipped_flag,
        source_group,
        is_skipped_flag_cume_dist,
        rows_in_source,
        is_skipped_flag_cume_dist_2,
        rows_in_track_id,
        seq_no,
        is_skipped_flag_vs_source_avg,
        first_known_ms_played,
        prev_ms_played,
        is_skipped_flag_vs_track_id_avg,
        running_ms_played,
        ms_played_sum_2_prior,
        ms_played_share_of_source,
        ms_played_share_of_track_id
    FROM windowed
    WHERE is_skipped_flag_rank <= 2
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    device,
    is_skipped_flag,
    source_group,
    is_skipped_flag_cume_dist,
    rows_in_source,
    is_skipped_flag_cume_dist_2,
    seq_no,
    is_skipped_flag_vs_source_avg,
    first_known_ms_played,
    prev_ms_played,
    is_skipped_flag_vs_track_id_avg,
    running_ms_played,
    ms_played_sum_2_prior,
    ms_played_share_of_source,
    LOWER(REGEXP_REPLACE(TRIM(source_group), ' ', '_')) AS source_group_slug,
    LEAST(rows_in_track_id, prev_ms_played) AS least_rows_in_track_id_prev_ms_played
FROM top_per_group
ORDER BY device NULLS LAST, listener_id DESC NULLS FIRST
