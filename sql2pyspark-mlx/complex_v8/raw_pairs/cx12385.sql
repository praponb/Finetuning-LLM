WITH cleaned AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        moods,
        UPPER(TRIM(title)) AS title_clean,
        LEAST(duration_sec, bpm) AS least_duration_sec_bpm
    FROM tracks
    WHERE (ARRAY_CONTAINS(moods, 'chill') OR artist_id <= 7)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        moods,
        title_clean,
        ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY least_duration_sec_bpm ASC, id) AS rn,
        duration_sec - LAG(duration_sec) OVER w_artist_id_by_least_duration AS duration_sec_change,
        MIN(least_duration_sec_bpm) OVER w_artist_id AS artist_id_min_least_duration_sec_bpm
    FROM cleaned
    WINDOW w_artist_id_by_least_duration AS (PARTITION BY artist_id ORDER BY least_duration_sec_bpm ASC, id), w_artist_id AS (PARTITION BY artist_id)
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        moods,
        title_clean,
        artist_id_min_least_duration_sec_bpm,
        duration_sec_change - bpm AS duration_sec_change_minus_bpm,
        COALESCE(bpm, 0) AS bpm_filled
    FROM sequenced
)
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    moods,
    title_clean,
    artist_id_min_least_duration_sec_bpm,
    bpm_filled,
    ROUND(duration_sec / NULLIF(bpm_filled, 0), 3) AS duration_sec_per_bpm_filled
FROM scored
WHERE title_clean IS NOT NULL
ORDER BY artist_id_min_least_duration_sec_bpm NULLS LAST
