WITH base AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        TRUNC(release_date, 'MM') AS release_date_month_start,
        DATE_TRUNC('YEAR', release_date) AS release_date_year
    FROM tracks
    WHERE duration_sec >= 144
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        release_date_year,
        COUNT(*) OVER (PARTITION BY YEAR(release_date_year)) AS rows_in_release_date_year_year,
        LEAD(duration_sec) OVER w_artist_id_release_date_month AS next_duration_sec,
        LAG(release_date_month_start, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date_year) ORDER BY duration_sec ASC, id) AS prev2_release_date_month_start,
        LEAD(duration_sec) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date_year) ORDER BY duration_sec ASC, id) AS next_duration_sec_2
    FROM base
    WINDOW w_artist_id_release_date_month AS (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date_month_start) ORDER BY release_date DESC NULLS FIRST, id)
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT artist_id, MIN(bpm) AS min_track_bpm
    FROM tracks
    GROUP BY artist_id
),
summarized_rows AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        t.min_track_bpm
    FROM metrics s
    LEFT JOIN track_stats t ON t.artist_id = s.artist_id
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        is_explicit,
        moods,
        release_date_year,
        rows_in_release_date_year_year,
        next_duration_sec,
        prev2_release_date_month_start,
        next_duration_sec_2,
        min_track_bpm,
        TO_DATE(release_date_year) AS release_date_year_date,
        MONTH(release_date) AS release_date_month
    FROM summarized_rows
    WHERE ARRAY_CONTAINS(moods, 'chill')
)
SELECT
    id,
    moods,
    release_date_year,
    rows_in_release_date_year_year,
    next_duration_sec_2,
    min_track_bpm,
    release_date_year_date,
    release_date_month,
    FLOOR(release_date_month / 2) * 2 AS release_date_month_bucket
FROM labeled
WHERE release_date_month BETWEEN 4 AND 9
ORDER BY next_duration_sec_2 DESC
