WITH cleaned AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        is_explicit,
        bpm,
        moods,
        DAYOFWEEK(release_date) AS release_date_dayofweek
    FROM tracks
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(release_date_dayofweek) OVER (PARTITION BY release_date_dayofweek, is_explicit) AS release_date_dayofweek_is_explicit_sum_release_d,
        MAX(release_date_dayofweek) OVER w_release_date_dayofweek_is_ex AS release_date_dayofweek_is_explicit_max_release_d,
        SIZE(COLLECT_SET(artist_id) OVER w_release_date_dayofweek_is_ex) AS distinct_artist_id_in_release_date_dayofweek_is_
    FROM cleaned
    WINDOW w_release_date_dayofweek_is_ex AS (PARTITION BY release_date_dayofweek, is_explicit)
),
filtered AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
        )
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        artist_id,
        moods,
        release_date_dayofweek,
        release_date_dayofweek_is_explicit_sum_release_d,
        release_date_dayofweek_is_explicit_max_release_d,
        distinct_artist_id_in_release_date_dayofweek_is_,
        ABS(bpm - 100) AS bpm_distance,
        ROUND(release_date_dayofweek / NULLIF(bpm, 0), 3) AS release_date_dayofweek_per_bpm,
        bpm - release_date_dayofweek AS bpm_minus_release_date_dayofweek
    FROM filtered
    WHERE release_date_dayofweek_is_explicit_max_release_d > 18
),
with_details AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.moods,
        s.distinct_artist_id_in_release_date_dayofweek_is_,
        s.bpm_distance,
        s.release_date_dayofweek_per_bpm,
        a.artist_name
    FROM derived s
    LEFT JOIN artists a ON a.id = s.artist_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(bpm_distance) IGNORE NULLS OVER (PARTITION BY artist_id ORDER BY bpm_distance ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_bpm_distance,
        bpm_distance - LAG(bpm_distance) OVER w_artist_id_by_bpm_distance AS bpm_distance_change,
        FIRST_VALUE(bpm_distance) IGNORE NULLS OVER (ORDER BY distinct_artist_id_in_release_date_dayofweek_is_ ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm_distance,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY distinct_artist_id_in_release_date_dayofweek_is_)) AS distinct_artist_id_in_distinct_artist_id_in_rele
    FROM with_details
    WINDOW w_artist_id_by_bpm_distance AS (PARTITION BY artist_id ORDER BY bpm_distance ASC, id)
)
SELECT
    id,
    artist_id,
    distinct_artist_id_in_release_date_dayofweek_is_,
    bpm_distance,
    release_date_dayofweek_per_bpm,
    artist_name,
    last_known_bpm_distance,
    distinct_artist_id_in_distinct_artist_id_in_rele,
    CONCAT(artist_name, '#', CAST(id AS STRING)) AS artist_name_ref
FROM with_windows
