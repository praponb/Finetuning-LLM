WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        ABS(duration_sec - 344) AS duration_sec_distance
    FROM tracks
    WHERE bpm < 165
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.ms_played) FROM plays p WHERE p.track_id = s.id) AS max_play_ms_played,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count
    FROM base s
),
labeled AS (
    -- Add calculated columns
    SELECT *, CONCAT(title, '#', CAST(duration_sec AS STRING)) AS title_ref
    FROM with_counts
    WHERE is_explicit = TRUE
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        duration_sec_distance - play_count AS duration_sec_distance_minus_play_count,
        GREATEST(max_play_ms_played, duration_sec) AS greatest_max_play_ms_played_duration_sec
    FROM labeled
    WHERE duration_sec <= 365
)
SELECT
    artist_id,
    is_explicit,
    play_count,
    GROUPING_ID() AS gid,
    ARRAY_SORT(COLLECT_SET(title)) AS title_list,
    SUM(CASE WHEN is_explicit = TRUE THEN 1 ELSE 0 END) AS n_is_explicit_true
FROM enriched
GROUP BY ROLLUP(artist_id, is_explicit, play_count)
ORDER BY artist_id NULLS LAST, is_explicit NULLS LAST, play_count NULLS LAST
