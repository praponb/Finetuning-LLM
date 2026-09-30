WITH valid_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source
    FROM plays
    WHERE device IS NOT NULL
      AND listener_id IS NOT NULL
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        played_at,
        ms_played,
        is_skipped,
        source,
        ROUND(CUME_DIST() OVER (PARTITION BY source ORDER BY played_at DESC, id), 3) AS played_at_cume_dist,
        ROUND(ms_played - AVG(ms_played) OVER w_listener_id_source, 2) AS ms_played_vs_listener_id_source_avg,
        MAX(ms_played) OVER (PARTITION BY source ORDER BY played_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS ms_played_max_3_prior,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY listener_id, source)) AS distinct_track_id_in_listener_id_source,
        SUM(ms_played) OVER (PARTITION BY is_skipped ORDER BY ms_played DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ms_played_sum_3,
        COUNT(*) OVER (PARTITION BY listener_id, TO_DATE(played_at)) AS rows_in_listener_id_played_at_to_date,
        SUM(ms_played) OVER w_track_id_source AS track_id_source_sum_ms_played,
        ROW_NUMBER() OVER w_source_played_at_to_date_by AS played_at_seq,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY listener_id, TO_DATE(played_at)), 2) AS ms_played_vs_listener_id_played_at_to_date_avg,
        FIRST_VALUE(ms_played) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played,
        ROUND(CUME_DIST() OVER (PARTITION BY source, TO_DATE(played_at) ORDER BY played_at, id), 3) AS played_at_cume_dist_2,
        ROW_NUMBER() OVER (PARTITION BY is_skipped ORDER BY ms_played DESC, id) AS ms_played_seq,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        ROW_NUMBER() OVER (PARTITION BY MONTH(played_at) ORDER BY played_at, id) AS seq_no,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER w_source_played_at_to_date_by)) / 60, 1) AS minutes_since_prev_played_at,
        ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY played_at, id) AS played_at_seq_2
    FROM valid_plays
    WINDOW w_listener_id_source AS (PARTITION BY listener_id, source), w_track_id_source AS (PARTITION BY track_id, source), w_source_played_at_to_date_by AS (PARTITION BY source, TO_DATE(played_at) ORDER BY played_at, id)
),
top_per_group AS (
    -- Top 5 per group
    SELECT *
    FROM windowed
    WHERE played_at_seq <= 5
      AND source = 'playlist'
)
SELECT
    id,
    played_at,
    ms_played,
    is_skipped,
    source,
    ms_played_vs_listener_id_source_avg,
    ms_played_max_3_prior,
    distinct_track_id_in_listener_id_source,
    ms_played_sum_3,
    rows_in_listener_id_played_at_to_date,
    track_id_source_sum_ms_played,
    played_at_seq,
    ms_played_vs_listener_id_played_at_to_date_avg,
    first_ms_played,
    played_at_cume_dist_2,
    ms_played_seq,
    second_ms_played,
    seq_no,
    minutes_since_prev_played_at,
    played_at_seq_2,
    CONCAT(source, '#', CAST(first_ms_played AS STRING)) AS source_ref,
    INITCAP(TRIM(source)) AS source_clean
FROM top_per_group
ORDER BY rows_in_listener_id_played_at_to_date
