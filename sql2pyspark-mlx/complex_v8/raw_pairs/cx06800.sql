WITH prepared AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label
    FROM tracks
    WHERE (SIZE(moods) >= 1 OR bpm < 143)
),
filtered AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM prepared s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND SIZE(t.moods) >= 2
              AND t.is_explicit = TRUE
        )
)
SELECT
    artist_id,
    is_explicit,
    COUNT(*) FILTER (WHERE NOT is_explicit) AS n_not_is_explicit,
    MAX(duration_sec) AS max_duration_sec,
    COUNT(*) AS n_rows
FROM filtered
GROUP BY GROUPING SETS ((artist_id, is_explicit), (artist_id), (is_explicit), ())
