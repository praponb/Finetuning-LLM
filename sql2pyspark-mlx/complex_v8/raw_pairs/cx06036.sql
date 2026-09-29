WITH scoped_tracks AS (
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
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm
    FROM tracks
    WHERE is_explicit = FALSE
),
with_parent_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        s.duration_sec_per_bpm,
        a.genre AS artist_genre,
        a.country AS artist_country,
        a.artist_name
    FROM scoped_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        artist_genre,
        artist_country,
        artist_name,
        LAST_VALUE(duration_sec_per_bpm) IGNORE NULLS OVER (PARTITION BY artist_genre, artist_id ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec_per_bpm,
        MAX(bpm) OVER (PARTITION BY is_explicit ORDER BY duration_sec DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS bpm_max_1_prior,
        LAG(duration_sec_per_bpm, 1, 0) OVER (ORDER BY bpm DESC, id) AS prev_duration_sec_per_bpm,
        SUM(duration_sec_per_bpm) OVER (PARTITION BY is_explicit) AS is_explicit_sum_duration_sec_per_bpm,
        MAX(duration_sec) OVER (PARTITION BY is_explicit) AS is_explicit_max_duration_sec,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_genre ORDER BY duration_sec), 3) AS duration_sec_percent_rank,
        COUNT(*) OVER (PARTITION BY artist_country) AS rows_in_artist_country,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY is_explicit), 2) AS duration_sec_vs_is_explicit_avg,
        ROUND(duration_sec_per_bpm - AVG(duration_sec_per_bpm) OVER (PARTITION BY is_explicit), 2) AS duration_sec_per_bpm_vs_is_explicit_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_genre, artist_id ORDER BY duration_sec DESC), 3) AS duration_sec_cume_dist
    FROM with_parent_artist
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        is_explicit,
        artist_name,
        prev_duration_sec_per_bpm,
        is_explicit_sum_duration_sec_per_bpm,
        is_explicit_max_duration_sec,
        duration_sec_percent_rank,
        duration_sec_per_bpm_vs_is_explicit_avg,
        duration_sec_cume_dist,
        SUBSTRING(TRIM(artist_name), 1, 3) AS artist_name_prefix,
        SUBSTRING(TRIM(artist_name), 1, 2) AS artist_name_prefix_2
    FROM with_windows
    WHERE duration_sec_cume_dist <= 0.6
),
stats AS (
    -- Summarise per group
    SELECT
        is_explicit,
        ARRAY_SORT(COLLECT_SET(artist_name)) AS artist_name_list,
        COUNT(*) FILTER (WHERE is_explicit = FALSE) AS n_is_explicit_false
    FROM annotated
    GROUP BY is_explicit
    HAVING AVG(prev_duration_sec_per_bpm) > 1.0
)
-- Final projection
SELECT
    is_explicit,
    n_is_explicit_false,
    IF(is_explicit, 'yes', 'no') AS is_explicit_label
FROM stats
WHERE SIZE(artist_name_list) >= 1
ORDER BY n_is_explicit_false DESC
