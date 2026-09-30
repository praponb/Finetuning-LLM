WITH cleaned_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        INITCAP(TRIM(title)) AS title_clean,
        duration_sec - bpm AS duration_sec_minus_bpm,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label
    FROM tracks
    WHERE release_date IS NOT NULL
),
labeled AS (
    -- Label and score rows
    SELECT *, ADD_MONTHS(release_date, 1) AS release_date_plus_1m
    FROM cleaned_tracks
    WHERE UPPER(TRIM(title_clean)) LIKE 'C%'
)
-- Subtotals with ROLLUP
SELECT
    artist_id,
    is_explicit,
    GROUPING_ID() AS gid,
    MAX(duration_sec) AS max_duration_sec,
    COUNT(DISTINCT artist_id) AS distinct_artist_ids,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec
FROM labeled
GROUP BY ROLLUP(artist_id, is_explicit)
ORDER BY artist_id NULLS LAST, is_explicit NULLS LAST
