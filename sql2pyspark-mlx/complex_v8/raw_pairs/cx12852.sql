WITH scoped_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec
    FROM tracks
    WHERE duration_sec BETWEEN 154 AND 352
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(bpm_per_duration_sec) IGNORE NULLS OVER (PARTITION BY artist_id ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm_per_duration_sec,
        MAX(bpm_per_duration_sec) OVER w_is_explicit_artist_id AS is_explicit_artist_id_max_bpm_per_duration_sec,
        LEAD(release_date) OVER (ORDER BY bpm_per_duration_sec DESC, id) AS next_release_date,
        LAG(duration_sec, 3, 0) OVER w_all_by_bpm_per_duration_sec AS prev3_duration_sec,
        MAX(bpm) OVER (PARTITION BY artist_id, is_explicit ORDER BY release_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS bpm_max_3,
        SUM(bpm_per_duration_sec) OVER (PARTITION BY is_explicit, artist_id) AS is_explicit_artist_id_sum_bpm_per_duration_sec,
        ROW_NUMBER() OVER w_artist_id_is_explicit_by_rel_2 AS rn,
        ROW_NUMBER() OVER (ORDER BY bpm_per_duration_sec DESC, id) AS bpm_per_duration_sec_seq,
        bpm_per_duration_sec - LAG(bpm_per_duration_sec) OVER w_artist_id_is_explicit_by_rel_2 AS bpm_per_duration_sec_change,
        MAX(duration_sec) OVER (PARTITION BY is_explicit, artist_id) AS is_explicit_artist_id_max_duration_sec
    FROM scoped_tracks
    WINDOW w_is_explicit_artist_id AS (PARTITION BY is_explicit, artist_id), w_all_by_bpm_per_duration_sec AS (ORDER BY bpm_per_duration_sec DESC, id), w_artist_id_is_explicit_by_rel_2 AS (PARTITION BY artist_id, is_explicit ORDER BY release_date, id)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        bpm_per_duration_sec,
        first_known_bpm_per_duration_sec,
        is_explicit_artist_id_max_bpm_per_duration_sec,
        next_release_date,
        prev3_duration_sec,
        bpm_max_3,
        is_explicit_artist_id_sum_bpm_per_duration_sec,
        bpm_per_duration_sec_seq,
        bpm_per_duration_sec_change,
        is_explicit_artist_id_max_duration_sec
    FROM sequenced
    WHERE rn = 1
      AND bpm_max_3 BETWEEN 132 AND 516
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    release_date,
    is_explicit,
    bpm,
    moods,
    bpm_per_duration_sec,
    prev3_duration_sec,
    is_explicit_artist_id_sum_bpm_per_duration_sec,
    bpm_per_duration_sec_seq,
    LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
FROM leaders
ORDER BY duration_sec, title NULLS LAST
