WITH recent_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag
    FROM tracks
    WHERE release_date BETWEEN DATE '2024-04-29' AND DATE '2025-08-28'
      AND bpm BETWEEN 75 AND 142
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        title_slug,
        ROUND(bpm / NULLIF(SUM(bpm) OVER w_is_explicit, 0), 4) AS bpm_share_of_is_explicit,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY is_explicit)) AS distinct_artist_id_in_is_explicit,
        LAG(duration_sec) OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec DESC, id) AS prev_duration_sec,
        NTILE(5) OVER w_artist_id_is_explicit_by_dur_2 AS duration_sec_quintile,
        ROUND(AVG(is_explicit_flag) OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS is_explicit_flag_avg_7,
        NTH_VALUE(duration_sec, 2) OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec
    FROM recent_tracks
    WINDOW w_is_explicit AS (PARTITION BY is_explicit), w_artist_id_is_explicit_by_dur_2 AS (PARTITION BY artist_id, is_explicit ORDER BY duration_sec, id)
),
without_matches AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
        )
)
SELECT
    id,
    title,
    duration_sec,
    is_explicit,
    bpm,
    moods,
    title_slug,
    bpm_share_of_is_explicit,
    distinct_artist_id_in_is_explicit,
    prev_duration_sec,
    second_duration_sec,
    LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug_2
FROM without_matches
ORDER BY duration_sec DESC
