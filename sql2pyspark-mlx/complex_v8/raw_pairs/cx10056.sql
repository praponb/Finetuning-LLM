WITH scoped_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        CASE WHEN duration_sec >= 357 THEN 'A' WHEN duration_sec >= 184 THEN 'B' ELSE 'C' END AS duration_sec_tier
    FROM tracks
    WHERE SIZE(moods) >= 1
      AND artist_id IS NOT NULL
),
summarized_rows AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE t.is_explicit = FALSE) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_is_explicit_count,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_count
    FROM scoped_tracks s
),
summarized_rows_2 AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_count_2,
        (SELECT COUNT(CASE WHEN t.is_explicit = FALSE THEN 1 END) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_is_explicit_count_2,
        (SELECT MIN(t.release_date) FROM tracks t WHERE t.artist_id = s.artist_id) AS first_track_release_date
    FROM summarized_rows s
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        duration_sec,
        is_explicit,
        bpm,
        duration_sec_tier,
        track_count,
        track_is_explicit_count_2,
        first_track_release_date,
        LEAST(duration_sec, track_count_2) AS least_duration_sec_track_count_2
    FROM summarized_rows_2
    WHERE duration_sec_tier NOT IN ('A', 'B')
)
SELECT
    id,
    is_explicit,
    duration_sec_tier,
    track_count,
    track_is_explicit_count_2,
    first_track_release_date
FROM enriched
ORDER BY track_count
