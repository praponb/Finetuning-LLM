WITH valid_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        duration_sec - bpm AS duration_sec_minus_bpm
    FROM tracks
    WHERE SIZE(moods) >= 2
      AND UPPER(TRIM(title)) LIKE 'R%'
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        duration_sec_minus_bpm,
        COALESCE(duration_sec_minus_bpm, 0) AS duration_sec_minus_bpm_filled,
        duration_sec_minus_bpm - bpm AS duration_sec_minus_bpm_minus_bpm
    FROM valid_tracks
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM labeled
    WHERE duration_sec_minus_bpm_filled > (SELECT MIN(duration_sec_minus_bpm_filled) FROM labeled)
)
SELECT
    is_explicit,
    artist_id,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS row_count,
    SUM(CASE WHEN LOWER(title) LIKE '%ho%' THEN 1 ELSE 0 END) AS n_lower_title_like_ho
FROM screened
GROUP BY CUBE(is_explicit, artist_id)
ORDER BY is_explicit NULLS LAST, artist_id NULLS LAST
