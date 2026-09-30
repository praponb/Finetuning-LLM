WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        country,
        debut_year,
        CONCAT(artist_name, '#', CAST(id AS STRING)) AS artist_name_ref,
        COALESCE(country, 'UNKNOWN') AS country_or_unknown
    FROM artists
    WHERE debut_year BETWEEN 1994 AND 2016
),
with_children_tracks AS (
    -- One row per track
    SELECT
        s.id,
        s.artist_name,
        s.country_or_unknown,
        t.id AS track_id,
        t.is_explicit AS track_is_explicit,
        t.release_date AS track_release_date
    FROM valid s
    JOIN tracks t ON t.artist_id = s.id
),
enriched AS (
    -- Label and score rows
    SELECT *, SUBSTRING(TRIM(artist_name), 1, 3) AS artist_name_prefix
    FROM with_children_tracks
    WHERE country_or_unknown NOT IN ('TH', 'UK')
)
SELECT
    track_is_explicit,
    COALESCE(country_or_unknown, 'ALL') AS country_or_unknown,
    ARRAY_SORT(COLLECT_SET(artist_name)) AS artist_name_list
FROM enriched
GROUP BY GROUPING SETS ((track_is_explicit, country_or_unknown), (track_is_explicit))
