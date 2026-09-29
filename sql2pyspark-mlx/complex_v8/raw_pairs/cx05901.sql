WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        SUBSTRING(TRIM(source), 1, 3) AS source_prefix
    FROM plays
    WHERE played_at < TIMESTAMP '2025-02-24 09:00:00'
),
without_matches AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
        )
),
joined AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.source_prefix,
        l.plan AS listener_plan,
        l.country AS listener_country,
        t.bpm AS track_bpm
    FROM without_matches s
    JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
),
filtered AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM joined s
    WHERE s.listener_id IN (
            SELECT p.listener_id
            FROM plays p
            WHERE p.played_at >= TIMESTAMP '2025-01-13 12:00:00'
              AND p.device <> 'desktop'
        )
),
with_parent AS (
    -- Attach track attributes
    SELECT
        s.*,
        t.title AS track_title,
        t.artist_id AS track_artist_id,
        t.duration_sec AS track_duration_sec,
        a.artist_name,
        a.country AS artist_country
    FROM filtered s
    JOIN tracks t ON t.id = s.track_id
    JOIN artists a ON a.id = t.artist_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        listener_country,
        track_bpm,
        track_title,
        track_artist_id,
        track_duration_sec,
        artist_name,
        artist_country,
        ROUND(AVG(track_bpm) OVER (PARTITION BY artist_country, track_id), 2) AS artist_country_track_id_avg_track_bpm,
        COUNT(*) OVER (PARTITION BY artist_country, track_id) AS rows_in_artist_country_track_id,
        FIRST_VALUE(track_bpm) OVER (PARTITION BY MONTH(played_at) ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_track_bpm,
        LAG(track_bpm) OVER (PARTITION BY MONTH(played_at) ORDER BY ms_played, id) AS prev_track_bpm,
        MIN(track_bpm) OVER (PARTITION BY artist_country, track_id) AS artist_country_track_id_min_track_bpm,
        MAX(track_duration_sec) OVER (PARTITION BY artist_country, track_id) AS artist_country_track_id_max_track_duration_sec
    FROM with_parent
),
latest AS (
    -- Keep the earliest row per track_artist_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        listener_country,
        track_bpm,
        track_title,
        track_artist_id,
        track_duration_sec,
        artist_name,
        artist_country,
        artist_country_track_id_avg_track_bpm,
        rows_in_artist_country_track_id,
        first_track_bpm,
        prev_track_bpm,
        artist_country_track_id_min_track_bpm,
        artist_country_track_id_max_track_duration_sec
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY track_artist_id ORDER BY played_at, id) AS rn
        FROM windowed
    ) ranked
    WHERE rn = 1
)
-- Subtotals with GROUPING SETS
SELECT
    listener_country,
    listener_id,
    track_id,
    GROUPING(listener_country) AS is_total_listener_country,
    COUNT(*) FILTER (WHERE device IN ('speaker', 'desktop')) AS n_device_in_speaker_desktop,
    MAX(played_at) AS last_played_at,
    MAX(rows_in_artist_country_track_id) AS max_rows_in_artist_country_track_id
FROM latest
GROUP BY GROUPING SETS ((listener_country, listener_id, track_id), (listener_country), (listener_id), ())
