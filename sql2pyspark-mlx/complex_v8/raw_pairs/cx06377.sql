WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit
    FROM tracks
    WHERE LENGTH(TRIM(title)) >= 4
      AND release_date < DATE '2025-03-08'
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        p.id AS play_id,
        p.source AS play_source,
        p.device AS play_device,
        p.ms_played AS play_ms_played,
        p.played_at AS play_played_at
    FROM filtered s
    JOIN plays p ON p.track_id = s.id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(play_ms_played) OVER (PARTITION BY play_id ORDER BY duration_sec DESC, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_play_ms_played,
        NTH_VALUE(duration_sec, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', play_played_at), play_source ORDER BY play_ms_played DESC, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER w_play_id_artist_id, 0), 4) AS duration_sec_share_of_play_id_artist_id
    FROM line_level
    WINDOW w_play_id_artist_id AS (PARTITION BY play_id, artist_id)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY release_date ASC), 3) AS release_date_percent_rank,
        LAG(play_ms_played, 1, 0) OVER (ORDER BY release_date ASC, id, play_id) AS prev_play_ms_played,
        SUM(second_duration_sec) OVER (PARTITION BY is_explicit, play_source) AS is_explicit_play_source_sum_second_duration_sec,
        SIZE(COLLECT_SET(play_id) OVER w_is_explicit_play_source) AS distinct_play_id_in_is_explicit_play_source,
        COUNT(*) OVER w_is_explicit_play_source AS rows_in_is_explicit_play_source,
        LAG(play_played_at) OVER (ORDER BY release_date ASC, id, play_id) AS prev_play_played_at
    FROM windowed
    WINDOW w_is_explicit_play_source AS (PARTITION BY is_explicit, play_source)
),
filtered_2 AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM with_trends s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.bpm < 142
              AND LOWER(t.title) LIKE '%eo%'
        )
      AND s.is_explicit = TRUE
)
SELECT
    id,
    artist_id,
    title,
    play_source,
    play_device,
    play_ms_played,
    play_played_at,
    second_duration_sec,
    release_date_percent_rank,
    is_explicit_play_source_sum_second_duration_sec,
    distinct_play_id_in_is_explicit_play_source,
    rows_in_is_explicit_play_source,
    prev_play_played_at
FROM filtered_2
WHERE play_source = 'album'
ORDER BY artist_id, second_duration_sec
