WITH scoped AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm
    FROM tracks
    WHERE duration_sec BETWEEN 144 AND 357
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        p.id AS play_id,
        p.source AS play_source,
        p.ms_played AS play_ms_played
    FROM scoped s
    JOIN plays p ON p.track_id = s.id
),
without_matches AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM line_level s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.ms_played >= 67932
        )
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(release_date) AS release_date_year,
        MIN(play_ms_played) AS min_play_ms_played,
        DATEDIFF(MAX(release_date), MIN(release_date)) AS release_date_span_days
    FROM without_matches
    GROUP BY 1
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY release_date_year) AS rows_in_release_date_year,
        SUM(release_date_year) OVER (PARTITION BY release_date_year) AS release_date_year_sum_release_date_year,
        ROUND(CUME_DIST() OVER (ORDER BY release_date_span_days ASC), 3) AS release_date_span_days_cume_dist
    FROM rollup_base
)
SELECT
    release_date_year,
    min_play_ms_played,
    release_date_span_days,
    rows_in_release_date_year,
    release_date_span_days_cume_dist,
    COALESCE(release_date_year, 0) AS release_date_year_filled
FROM windowed
ORDER BY min_play_ms_played DESC NULLS FIRST, release_date_year
LIMIT 10
