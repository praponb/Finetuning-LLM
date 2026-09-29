WITH prepared_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        moods,
        COALESCE(bpm, 0) AS bpm_filled
    FROM tracks
    WHERE bpm >= 85
      AND artist_id IS NOT NULL
),
joined_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.moods,
        a.artist_name
    FROM prepared_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id AND LENGTH(TRIM(a.artist_name)) >= 8
),
qualified AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM joined_artist s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.source = 'playlist'
              AND p.listener_id IS NOT NULL
        )
      AND s.artist_id >= 4
),
flattened AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.artist_name,
        t.mood
    FROM qualified s
    LATERAL VIEW OUTER EXPLODE(s.moods) t AS mood
)
SELECT
    artist_id,
    mood,
    is_explicit,
    GROUPING_ID() AS grouping_level,
    SUM(duration_sec) AS total_duration_sec
FROM flattened
GROUP BY CUBE(artist_id, mood, is_explicit)
