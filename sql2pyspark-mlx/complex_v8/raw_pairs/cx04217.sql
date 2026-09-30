WITH filtered AS (
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
        SUBSTRING(TRIM(title), 1, 4) AS title_prefix,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'R%'
      AND ARRAY_CONTAINS(moods, 'sad')
),
with_parent_artist AS (
    -- Join artists
    SELECT s.*, a.artist_name, a.debut_year AS artist_debut_year
    FROM filtered s
    JOIN artists a ON a.id = s.artist_id
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_TRUNC('QUARTER', release_date) AS release_date_quarter,
        CASE WHEN duration_sec >= 392 THEN 'A' WHEN duration_sec >= 164 THEN 'B' ELSE 'C' END AS duration_sec_tier
    FROM with_parent_artist
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, metric_value
FROM (
    SELECT id, bpm, duration_sec, artist_debut_year
    FROM labeled
) src
UNPIVOT (
    metric_value FOR field IN (bpm, duration_sec, artist_debut_year)
) unp
