WITH scoped AS (
    -- Scope the tracks rows
    SELECT *
    FROM tracks
    WHERE duration_sec < 352
      AND UPPER(TRIM(title)) LIKE 'S%'
),
unioned AS (
    -- Stack the branches
    SELECT
        'track' AS source,
        bpm AS ref_id,
        release_date AS event_date,
        title AS label
    FROM scoped
    WHERE is_explicit = TRUE
    UNION ALL
    SELECT
        'track_2' AS source,
        id AS ref_id,
        release_date AS event_date,
        title AS label
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'party')
)
SELECT source, COUNT(*) AS n_rows, SUM(ref_id) AS total_ref_id, MAX(event_date) AS latest_event_date
FROM unioned
GROUP BY source
ORDER BY source
