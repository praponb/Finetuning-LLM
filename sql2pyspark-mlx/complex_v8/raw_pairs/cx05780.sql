WITH filtered AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        CONCAT(device, '#', CAST(listener_id AS STRING)) AS device_ref,
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM plays
    WHERE device = 'desktop'
      AND is_skipped = FALSE
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(p.ms_played), 2) FROM plays p WHERE p.listener_id = s.listener_id) AS avg_play_ms_played,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS last_played_at,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.listener_id) AS play_count
    FROM filtered s
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device_ref,
        ms_played_filled,
        last_played_at,
        play_count,
        NTILE(4) OVER (ORDER BY play_count, id) AS play_count_quartile,
        play_count - LAG(play_count) OVER w_all_by_play_count AS play_count_change,
        SUM(ms_played) OVER (ORDER BY play_count ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played,
        SIZE(COLLECT_SET(listener_id) OVER w_track_id_last_played_at_mont) AS distinct_listener_id_in_track_id_last_played_at_,
        ms_played - LAG(ms_played) OVER (ORDER BY play_count ASC, id) AS ms_played_change
    FROM with_counts
    WINDOW w_all_by_play_count AS (ORDER BY play_count ASC, id), w_track_id_last_played_at_mont AS (PARTITION BY track_id, MONTH(last_played_at))
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        track_id,
        ms_played,
        last_played_at,
        play_count,
        play_count_quartile,
        play_count_change,
        running_ms_played,
        distinct_listener_id_in_track_id_last_played_at_,
        ms_played_change,
        CONCAT(device_ref, '#', CAST(play_count_quartile AS STRING)) AS device_ref_ref,
        COALESCE(play_count_change, 0) AS play_count_change_filled
    FROM with_trends
),
with_counts_2 AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.track_id) AS play_count_2,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.track_id = s.track_id) AS first_played_at,
        (SELECT MIN(p.ms_played) FROM plays p WHERE p.track_id = s.track_id) AS min_play_ms_played
    FROM labeled s
),
joined_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.track_id,
        s.ms_played,
        s.last_played_at,
        s.play_count,
        s.play_count_change,
        s.running_ms_played,
        s.distinct_listener_id_in_track_id_last_played_at_,
        s.ms_played_change,
        s.play_count_change_filled,
        s.play_count_2,
        s.first_played_at,
        s.min_play_ms_played,
        t.release_date AS track_release_date
    FROM with_counts_2 s
    JOIN tracks t ON t.id = s.track_id
    WHERE t.release_date IS NOT NULL
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        ADD_MONTHS(track_release_date, 3) AS track_release_date_plus_3m,
        LAST_DAY(track_release_date) AS track_release_date_month_end
    FROM joined_track
)
-- Final projection
SELECT
    id,
    track_id,
    ms_played,
    last_played_at,
    play_count,
    play_count_change,
    running_ms_played,
    distinct_listener_id_in_track_id_last_played_at_,
    ms_played_change,
    play_count_change_filled,
    play_count_2,
    min_play_ms_played,
    track_release_date,
    track_release_date_plus_3m,
    track_release_date_month_end
FROM derived
ORDER BY track_release_date DESC
