WITH cleaned_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        moods,
        SUBSTRING(TRIM(title), 1, 3) AS title_prefix,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
    WHERE SIZE(moods) >= 2
),
with_stats AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN t.is_explicit = FALSE THEN 1 END) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_is_explicit_count,
        (SELECT MAX(t.duration_sec) FROM tracks t WHERE t.artist_id = s.artist_id) AS max_track_duration_sec,
        (SELECT COUNT(CASE WHEN t.is_explicit = FALSE THEN 1 END) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_is_explicit_count_2
    FROM cleaned_tracks s
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM with_stats
    WHERE title_len < (SELECT MAX(title_len) FROM with_stats)
),
stats AS (
    -- Group-level totals
    SELECT
        CASE WHEN max_track_duration_sec >= 304 THEN 'A' WHEN max_track_duration_sec >= 238 THEN 'B' ELSE 'C' END AS max_track_duration_sec_tier,
        artist_id,
        ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec,
        COUNT(DISTINCT id) AS distinct_ids
    FROM filtered
    GROUP BY 1, 2
)
SELECT max_track_duration_sec_tier, artist_id, distinct_ids
FROM stats
ORDER BY artist_id DESC, distinct_ids DESC
