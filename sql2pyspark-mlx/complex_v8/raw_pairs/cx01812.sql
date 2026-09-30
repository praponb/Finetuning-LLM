WITH recent_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE NOT is_explicit
      AND artist_id IS NOT NULL
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        bpm + duration_sec AS bpm_plus_duration_sec,
        MONTH(release_date) AS release_date_month,
        DATE_TRUNC('YEAR', release_date) AS release_date_year
    FROM recent_tracks
),
artist_id_stats AS (
    -- Max release_date_month per artist_id
    SELECT artist_id, MAX(release_date_month) AS max_release_date_month
    FROM scored
    GROUP BY artist_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_release_date_month,
        ROUND(s.release_date_month - g.max_release_date_month, 2) AS release_date_month_minus_max
    FROM scored s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.release_date_month = g.max_release_date_month
),
combined_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.moods,
        s.release_date_month,
        s.max_release_date_month,
        s.release_date_month_minus_max,
        a.country AS artist_country
    FROM vs_group s
    JOIN artists a ON a.id = s.artist_id AND a.genre IN ('Pop', 'Classical')
    WHERE a.country <> 'TH'
)
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    release_date,
    moods,
    release_date_month,
    max_release_date_month,
    artist_country,
    COALESCE(release_date_month, 0) AS release_date_month_filled
FROM combined_details_artist
