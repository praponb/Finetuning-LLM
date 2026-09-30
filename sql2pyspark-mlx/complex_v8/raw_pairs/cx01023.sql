WITH prepared_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        ARRAY_CONTAINS(moods, 'chill') AS has_chill
    FROM tracks
    WHERE SIZE(moods) >= 1
),
deduped AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        has_chill
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY bpm DESC, id DESC) AS row_num
        FROM prepared_tracks
    ) ranked
    WHERE row_num = 1
)
-- Subtotals with GROUPING SETS
SELECT
    has_chill,
    artist_id,
    is_explicit,
    SUM(CASE WHEN LOWER(title) LIKE '%ho%' THEN 1 ELSE 0 END) AS n_lower_title_like_ho
FROM deduped
GROUP BY GROUPING SETS ((has_chill, artist_id, is_explicit), (has_chill), (artist_id))
ORDER BY has_chill NULLS LAST, artist_id NULLS LAST, is_explicit NULLS LAST
