WITH recent_tracks AS (
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
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag
    FROM tracks
    WHERE is_explicit = TRUE
),
with_parent_artist AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.is_verified AS artist_is_verified,
        a.debut_year AS artist_debut_year
    FROM recent_tracks s
    JOIN artists a ON a.id = s.artist_id
),
with_parent_artist_2 AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.moods,
        s.is_explicit_flag,
        s.artist_debut_year,
        a.debut_year AS artist_debut_year_2,
        a.country AS artist_country
    FROM with_parent_artist s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.debut_year < 2015
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(artist_country) OVER (PARTITION BY is_explicit_flag, artist_id)) AS distinct_artist_country_in_is_explicit_flag_arti,
        ROUND(PERCENT_RANK() OVER w_is_explicit_by_release_date, 3) AS release_date_percent_rank
    FROM with_parent_artist_2
    WINDOW w_is_explicit_by_release_date AS (PARTITION BY is_explicit ORDER BY release_date DESC)
)
SELECT *
FROM (
    SELECT artist_id, is_explicit_flag, artist_country, distinct_artist_country_in_is_explicit_flag_arti
    FROM metrics
) src
PIVOT (
    SUM(distinct_artist_country_in_is_explicit_flag_arti)
    FOR artist_country IN ('KR', 'JP')
)
