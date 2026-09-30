WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_label,
        bpm - duration_sec AS bpm_minus_duration_sec,
        LEAST(bpm, duration_sec) AS least_bpm_duration_sec
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'P%'
      AND release_date BETWEEN DATE '2024-08-26' AND DATE '2025-08-28'
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.bpm,
        s.release_date_label,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.device AS play_device,
        p.played_at AS play_played_at
    FROM prepared s
    JOIN plays p ON p.track_id = s.id AND p.device IN ('tv', 'desktop')
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(play_ms_played, 2) OVER (PARTITION BY artist_id ORDER BY play_played_at ASC, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_play_ms_played,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id ORDER BY play_played_at ASC), 3) AS play_played_at_percent_rank,
        SIZE(COLLECT_SET(play_id) OVER (PARTITION BY artist_id, play_device)) AS distinct_play_id_in_artist_id_play_device
    FROM detail_rows_plays
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        title,
        bpm,
        release_date_label,
        play_id,
        play_ms_played,
        play_device,
        second_play_ms_played,
        play_played_at_percent_rank,
        distinct_play_id_in_artist_id_play_device,
        SUM(play_ms_played) OVER (PARTITION BY distinct_play_id_in_artist_id_play_device) AS distinct_play_id_in_artist_id_play_device_sum_pl,
        RANK() OVER (PARTITION BY play_id ORDER BY bpm, id, play_id) AS bpm_rank,
        COUNT(*) OVER w_distinct_play_id_in_artist_i AS rows_in_distinct_play_id_in_artist_id_play_devic,
        MIN(duration_sec) OVER w_distinct_play_id_in_artist_i AS distinct_play_id_in_artist_id_play_device_min_du
    FROM sequenced
    WINDOW w_distinct_play_id_in_artist_i AS (PARTITION BY distinct_play_id_in_artist_id_play_device)
),
with_activity AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM windowed s
    WHERE s.artist_id IN (
            SELECT t.artist_id
            FROM tracks t
            WHERE t.bpm < 127
              AND SIZE(t.moods) >= 1
        )
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT distinct_play_id_in_artist_id_play_device, play_device, second_play_ms_played
    FROM with_activity
) src
PIVOT (
    COUNT(second_play_ms_played)
    FOR play_device IN ('speaker', 'desktop', 'phone')
)
