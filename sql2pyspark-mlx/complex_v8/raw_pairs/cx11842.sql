WITH prepared_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        DATE_TRUNC('QUARTER', release_date) AS release_date_quarter,
        DATE_TRUNC('QUARTER', release_date) AS release_date_quarter_2
    FROM tracks
    WHERE NOT is_explicit
      AND title IS NOT NULL
),
filtered AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM prepared_tracks s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
        )
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY duration_sec DESC), 3) AS duration_sec_cume_dist,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY is_explicit), 0), 4) AS duration_sec_share_of_is_explicit,
        ROUND(PERCENT_RANK() OVER (ORDER BY duration_sec DESC, id), 3) AS duration_sec_percent_rank,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY artist_id ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec,
        FIRST_VALUE(duration_sec) OVER (PARTITION BY artist_id ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec,
        ROUND(duration_sec - AVG(duration_sec) OVER w_is_explicit, 2) AS duration_sec_vs_is_explicit_avg
    FROM filtered
    WINDOW w_is_explicit AS (PARTITION BY is_explicit)
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.is_explicit,
        s.duration_sec_cume_dist,
        s.duration_sec_percent_rank,
        s.first_duration_sec,
        p.id AS play_id,
        p.source AS play_source,
        p.is_skipped AS play_is_skipped,
        p.device AS play_device,
        p.listener_id AS play_listener_id
    FROM sequenced s
    JOIN plays p ON p.track_id = s.id AND p.source IN ('search', 'radio', 'album')
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM line_level
    WHERE duration_sec > (SELECT AVG(duration_sec) FROM line_level)
),
with_activity_2 AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM with_activity s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
        )
),
sequenced_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        duration_sec,
        is_explicit,
        duration_sec_cume_dist,
        duration_sec_percent_rank,
        first_duration_sec,
        play_id,
        play_source,
        play_device,
        play_listener_id,
        MIN(duration_sec_percent_rank) OVER (PARTITION BY play_device, play_id ORDER BY first_duration_sec DESC, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_duration_sec_percent_rank_so_far,
        MIN(duration_sec) OVER (PARTITION BY play_is_skipped) AS play_is_skipped_min_duration_sec,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY play_is_skipped), 0), 4) AS duration_sec_share_of_play_is_skipped,
        first_duration_sec - LAG(first_duration_sec) OVER (PARTITION BY play_device, play_id ORDER BY first_duration_sec DESC, id, play_id) AS first_duration_sec_change
    FROM with_activity_2
)
SELECT
    id,
    duration_sec,
    is_explicit,
    duration_sec_cume_dist,
    duration_sec_percent_rank,
    first_duration_sec,
    play_id,
    play_source,
    play_device,
    play_listener_id,
    min_duration_sec_percent_rank_so_far,
    play_is_skipped_min_duration_sec,
    duration_sec_share_of_play_is_skipped,
    first_duration_sec_change,
    ROUND(duration_sec_cume_dist * 1.1, 2) AS duration_sec_cume_dist_adj,
    LOWER(REGEXP_REPLACE(TRIM(play_device), ' ', '_')) AS play_device_slug
FROM sequenced_2
