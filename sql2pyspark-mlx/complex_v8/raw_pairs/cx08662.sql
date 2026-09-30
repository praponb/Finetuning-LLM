WITH prepared_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        CONCAT(title, '#', CAST(artist_id AS STRING)) AS title_ref
    FROM tracks
    WHERE bpm >= 94
),
with_details AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.bpm,
        s.moods,
        s.title_ref,
        a.artist_name
    FROM prepared_tracks s
    JOIN artists a ON a.id = s.artist_id
    WHERE UPPER(TRIM(a.artist_name)) LIKE 'N%'
),
rollup_base AS (
    -- Summarise per group
    SELECT
        artist_id,
        ARRAY_SORT(COLLECT_SET(artist_name)) AS artist_name_list,
        ROUND(100 * COUNT(CASE WHEN UPPER(TRIM(artist_name)) LIKE 'S%' THEN 1 END) / COUNT(*), 1) AS pct_upper_trim_artist_name_like,
        ROUND(AVG(bpm), 2) AS avg_bpm,
        SUM(bpm) AS total_bpm,
        MIN(bpm) AS min_bpm
    FROM with_details
    GROUP BY artist_id
    HAVING MAX(duration_sec) > 184
)
SELECT artist_id, measure, metric_value
FROM (
    SELECT artist_id, pct_upper_trim_artist_name_like, avg_bpm
    FROM rollup_base
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR measure IN (pct_upper_trim_artist_name_like, avg_bpm)
) unp
