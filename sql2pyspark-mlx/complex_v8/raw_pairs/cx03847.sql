WITH prepared_tracks AS (
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
        GREATEST(bpm, duration_sec) AS greatest_bpm_duration_sec,
        CONCAT(title, '#', CAST(id AS STRING)) AS title_ref,
        SIZE(moods) AS moods_count
    FROM tracks
    WHERE NOT is_explicit
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.greatest_bpm_duration_sec,
        s.title_ref,
        s.moods_count,
        a.artist_name
    FROM prepared_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
)
SELECT
    is_explicit,
    artist_id,
    ROUND(100 * COUNT(CASE WHEN LENGTH(TRIM(artist_name)) >= 5 THEN 1 END) / COUNT(*), 1) AS pct_length_trim_artist_name_5
FROM with_details
GROUP BY GROUPING SETS ((is_explicit, artist_id), (is_explicit))
ORDER BY is_explicit NULLS LAST, artist_id NULLS LAST
