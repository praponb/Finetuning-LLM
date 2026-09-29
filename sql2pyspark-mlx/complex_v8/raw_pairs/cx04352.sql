WITH recent AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        CASE WHEN duration_sec >= 392 THEN 'HIGH' WHEN duration_sec >= 164 THEN 'MEDIUM' ELSE 'LOW' END AS duration_sec_tier,
        DATE_ADD(release_date, 7) AS release_date_plus_7d
    FROM tracks
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        COUNT(*) AS track_count,
        MIN(release_date) AS first_track_release_date,
        MIN(release_date) AS first_track_release_date_2
    FROM tracks
    GROUP BY artist_id
),
profiled AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        COALESCE(t.track_count, 0) AS track_count,
        t.first_track_release_date,
        t.first_track_release_date_2
    FROM recent s
    LEFT JOIN track_stats t ON t.artist_id = s.artist_id
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.duration_sec_tier,
        s.release_date_plus_7d,
        s.track_count,
        s.first_track_release_date,
        s.first_track_release_date_2,
        p.id AS play_id,
        p.device AS play_device,
        p.source AS play_source,
        p.is_skipped AS play_is_skipped
    FROM profiled s
    LEFT JOIN plays p ON p.track_id = s.id
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        DATE_TRUNC('QUARTER', release_date_plus_7d) AS release_date_plus_7d_quarter
    FROM detail_rows_plays
    WHERE play_source NOT IN ('album', 'playlist')
),
enriched_artist AS (
    -- Enrich with dimension data
    SELECT s.*, a.country AS artist_country, a.debut_year AS artist_debut_year
    FROM derived s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.debut_year <= 2006
)
SELECT
    id,
    artist_id,
    is_explicit,
    bpm,
    moods,
    duration_sec_tier,
    track_count,
    first_track_release_date,
    first_track_release_date_2,
    play_id,
    play_device,
    play_is_skipped,
    release_date_plus_7d_quarter,
    artist_country,
    artist_debut_year,
    GREATEST(artist_debut_year, track_count) AS greatest_artist_debut_year_track_count,
    DATEDIFF(release_date_plus_7d, first_track_release_date) AS days_first_track_release_date_to_release_date_pl
FROM enriched_artist
WHERE play_source <> 'playlist'
ORDER BY artist_country, release_date_plus_7d_quarter
