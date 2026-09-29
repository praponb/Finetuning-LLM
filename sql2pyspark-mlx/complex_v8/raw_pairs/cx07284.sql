WITH scoped_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix
    FROM tracks
),
combined_details_artist AS (
    -- Attach artist attributes
    SELECT s.*, a.is_verified AS artist_is_verified, a.artist_name
    FROM scoped_tracks s
    JOIN artists a ON a.id = s.artist_id AND a.country = 'US'
    WHERE a.artist_name IS NOT NULL
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        title_prefix,
        artist_is_verified,
        SUM(duration_sec) OVER w_is_explicit AS is_explicit_sum_duration_sec,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_is_verified ORDER BY duration_sec DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_3,
        SIZE(COLLECT_SET(artist_id) OVER w_is_explicit) AS distinct_artist_id_in_is_explicit,
        NTH_VALUE(duration_sec, 2) OVER (PARTITION BY artist_is_verified, is_explicit ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY artist_is_verified ORDER BY duration_sec DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec,
        ROUND(CUME_DIST() OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec ASC), 3) AS duration_sec_cume_dist,
        LAG(bpm) OVER (PARTITION BY artist_is_verified ORDER BY duration_sec DESC NULLS FIRST, id) AS prev_bpm,
        MIN(duration_sec) OVER (PARTITION BY artist_id, is_explicit) AS artist_id_is_explicit_min_duration_sec,
        bpm - LAG(bpm) OVER w_artist_is_verified_by_durati_3 AS bpm_change
    FROM combined_details_artist
    WINDOW w_is_explicit AS (PARTITION BY is_explicit), w_artist_is_verified_by_durati_3 AS (PARTITION BY artist_is_verified ORDER BY duration_sec DESC NULLS FIRST, id)
)
SELECT
    id,
    artist_id,
    is_explicit,
    title_prefix,
    bpm_avg_3,
    distinct_artist_id_in_is_explicit,
    second_duration_sec,
    first_known_duration_sec,
    duration_sec_cume_dist,
    prev_bpm,
    artist_id_is_explicit_min_duration_sec,
    bpm_change
FROM with_windows
ORDER BY first_known_duration_sec DESC
