WITH scoped AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm,
        LEAST(bpm, duration_sec) AS least_bpm_duration_sec
    FROM tracks
    WHERE duration_sec > 144
),
latest_per_key AS (
    -- Keep the earliest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        duration_sec_per_bpm,
        least_bpm_duration_sec
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY bpm, id) AS row_num
        FROM scoped
    ) ranked
    WHERE row_num = 1
)
-- Subtotals with ROLLUP
SELECT
    is_explicit,
    artist_id,
    ROUND(SUM(bpm) / NULLIF(SUM(duration_sec_per_bpm), 0), 3) AS bpm_to_duration_sec_per_bpm_ratio,
    COUNT(*) AS row_count,
    COUNT(DISTINCT id) AS distinct_ids
FROM latest_per_key
GROUP BY ROLLUP(is_explicit, artist_id)
ORDER BY is_explicit NULLS LAST, artist_id NULLS LAST
