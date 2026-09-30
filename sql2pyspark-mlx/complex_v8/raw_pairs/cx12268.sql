WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        moods,
        bpm - duration_sec AS bpm_minus_duration_sec,
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm,
        DATEDIFF(DATE '2025-12-31', release_date) AS days_since_release_date
    FROM tracks
    WHERE release_date < DATE '2025-08-02'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        is_explicit,
        bpm,
        bpm_minus_duration_sec,
        FIRST_VALUE(bpm_minus_duration_sec) IGNORE NULLS OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec_per_bpm DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm_minus_duration_sec,
        LAG(duration_sec_per_bpm, 3, 0) OVER (PARTITION BY is_explicit ORDER BY bpm_minus_duration_sec DESC, id) AS prev3_duration_sec_per_bpm,
        LAST_VALUE(bpm) OVER (PARTITION BY is_explicit ORDER BY bpm_minus_duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm,
        ROUND(AVG(days_since_release_date) OVER w_is_explicit_artist_id, 2) AS is_explicit_artist_id_avg_days_since_release_dat
    FROM recent
    WINDOW w_is_explicit_artist_id AS (PARTITION BY is_explicit, artist_id)
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.is_explicit,
        s.bpm,
        s.is_explicit_artist_id_avg_days_since_release_dat,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.ms_played AS play_ms_played
    FROM ranked s
    JOIN plays p ON p.track_id = s.id
)
-- Subtotals with CUBE
SELECT
    artist_id,
    play_id,
    GROUPING_ID() AS gid,
    COUNT(*) AS cnt,
    SUM(CASE WHEN is_explicit = TRUE THEN 1 ELSE 0 END) AS n_is_explicit_true
FROM detail_rows
GROUP BY CUBE(artist_id, play_id)
ORDER BY artist_id NULLS LAST, play_id NULLS LAST
