WITH valid AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        CONCAT_WS(': ', device, source) AS device_source_label,
        DAYOFWEEK(played_at) AS played_at_dayofweek
    FROM plays
    WHERE track_id IS NOT NULL
      AND is_skipped = FALSE
),
enriched_track AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        s.device_source_label,
        s.played_at_dayofweek,
        t.bpm AS track_bpm,
        t.artist_id AS track_artist_id,
        t.duration_sec AS track_duration_sec
    FROM valid s
    JOIN tracks t ON t.id = s.track_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        device,
        device_source_label,
        track_bpm,
        track_artist_id,
        LAST_VALUE(played_at_dayofweek) IGNORE NULLS OVER (ORDER BY track_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at_dayofweek,
        SUM(played_at_dayofweek) OVER (PARTITION BY is_skipped) AS is_skipped_sum_played_at_dayofweek,
        ROW_NUMBER() OVER (ORDER BY track_duration_sec, id) AS rn,
        LEAD(track_duration_sec) OVER (PARTITION BY device, played_at_dayofweek ORDER BY played_at_dayofweek ASC, id) AS next_track_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY track_artist_id ORDER BY played_at_dayofweek, id) AS rn_2,
        DENSE_RANK() OVER (PARTITION BY device, played_at_dayofweek ORDER BY played_at_dayofweek ASC) AS played_at_dayofweek_dense_rank,
        FIRST_VALUE(played_at_dayofweek) OVER (PARTITION BY device, played_at_dayofweek ORDER BY played_at_dayofweek ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_played_at_dayofweek,
        COUNT(*) OVER (PARTITION BY device, track_id) AS rows_in_device_track_id,
        DENSE_RANK() OVER w_all_by_track_duration_sec_ti AS track_duration_sec_dense_rank,
        SIZE(COLLECT_SET(device) OVER w_is_skipped) AS distinct_device_in_is_skipped,
        LAST_VALUE(track_bpm) OVER (PARTITION BY device, played_at_dayofweek ORDER BY played_at_dayofweek ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_track_bpm,
        LEAD(played_at_dayofweek, 1, 0) OVER w_track_artist_id_by_played_at AS next_played_at_dayofweek
    FROM enriched_track
    WINDOW w_all_by_track_duration_sec_ti AS (ORDER BY track_duration_sec), w_is_skipped AS (PARTITION BY is_skipped), w_track_artist_id_by_played_at AS (PARTITION BY track_artist_id ORDER BY played_at_dayofweek, id), w_all_by_track_duration_sec AS (ORDER BY track_duration_sec, id)
),
first_rows AS (
    -- First row per group
    SELECT
        id,
        listener_id,
        track_id,
        device,
        device_source_label,
        track_bpm,
        track_artist_id,
        last_known_played_at_dayofweek,
        is_skipped_sum_played_at_dayofweek,
        next_track_duration_sec,
        rn_2,
        played_at_dayofweek_dense_rank,
        first_played_at_dayofweek,
        rows_in_device_track_id,
        track_duration_sec_dense_rank,
        distinct_device_in_is_skipped,
        last_track_bpm,
        next_played_at_dayofweek
    FROM metrics
    WHERE rn = 1
      AND distinct_device_in_is_skipped BETWEEN 2 AND 8
),
metrics_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_all_by_rows_in_device_track, 3) AS rows_in_device_track_id_percent_rank,
        ROUND(CUME_DIST() OVER (ORDER BY rows_in_device_track_id DESC), 3) AS rows_in_device_track_id_cume_dist,
        ROUND(next_played_at_dayofweek - AVG(next_played_at_dayofweek) OVER (PARTITION BY track_artist_id), 2) AS next_played_at_dayofweek_vs_track_artist_id_avg,
        COUNT(*) OVER (PARTITION BY track_artist_id) AS rows_in_track_artist_id,
        MAX(rn_2) OVER w_track_artist_id AS track_artist_id_max_rn_2,
        NTILE(3) OVER (PARTITION BY first_played_at_dayofweek, track_artist_id ORDER BY rn_2 NULLS LAST, id) AS rn_2_tercile
    FROM first_rows
    WINDOW w_all_by_rows_in_device_track AS (ORDER BY rows_in_device_track_id DESC, id), w_track_artist_id AS (PARTITION BY track_artist_id)
),
totals AS (
    -- Summarise per group
    SELECT
        track_duration_sec_dense_rank,
        last_known_played_at_dayofweek,
        first_played_at_dayofweek,
        ROUND(SUM(rows_in_device_track_id_cume_dist), 2) AS total_rows_in_device_track_id_cume_dist,
        COUNT(DISTINCT track_artist_id) AS distinct_track_artist_ids,
        SUM(CASE WHEN device NOT IN ('tv', 'speaker') THEN rows_in_device_track_id ELSE 0 END) AS rows_in_device_track_id_device_not_in_tv_speaker,
        ARRAY_SORT(COLLECT_SET(device)) AS device_list
    FROM metrics_2
    GROUP BY track_duration_sec_dense_rank, last_known_played_at_dayofweek, first_played_at_dayofweek
)
-- Final projection
SELECT track_duration_sec_dense_rank, first_played_at_dayofweek, device_list
FROM totals
ORDER BY track_duration_sec_dense_rank DESC NULLS FIRST, first_played_at_dayofweek
