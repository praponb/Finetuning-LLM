WITH base_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        duration_sec + bpm AS duration_sec_plus_bpm,
        DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label
    FROM tracks
    WHERE (release_date BETWEEN DATE '2024-05-12' AND DATE '2025-08-28' OR duration_sec > 239)
),
with_parent AS (
    -- Join artists
    SELECT s.*, a.artist_name, a.is_verified AS artist_is_verified
    FROM base_tracks s
    JOIN artists a ON a.id = s.artist_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        duration_sec_plus_bpm - LAG(duration_sec_plus_bpm) OVER w_is_explicit_by_duration_sec AS duration_sec_plus_bpm_change,
        MAX(duration_sec_plus_bpm) OVER (PARTITION BY is_explicit ORDER BY duration_sec DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS duration_sec_plus_bpm_max_7,
        SIZE(COLLECT_SET(artist_id) OVER w_is_explicit_artist_is_verifi) AS distinct_artist_id_in_is_explicit_artist_is_veri,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_is_verified, artist_id), 2) AS artist_is_verified_artist_id_avg_duration_sec,
        NTILE(5) OVER (PARTITION BY artist_is_verified ORDER BY bpm DESC, id) AS bpm_quintile,
        MIN(duration_sec_plus_bpm) OVER (PARTITION BY artist_is_verified, artist_id) AS artist_is_verified_artist_id_min_duration_sec_pl,
        SUM(duration_sec) OVER w_artist_is_verified_artist_id AS artist_is_verified_artist_id_sum_duration_sec,
        SUM(duration_sec_plus_bpm) OVER (PARTITION BY is_explicit ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec_plus_bpm,
        SUM(duration_sec) OVER w_is_explicit_artist_is_verifi AS is_explicit_artist_is_verified_sum_duration_sec
    FROM with_parent
    WINDOW w_is_explicit_by_duration_sec AS (PARTITION BY is_explicit ORDER BY duration_sec DESC, id), w_is_explicit_artist_is_verifi AS (PARTITION BY is_explicit, artist_is_verified), w_artist_is_verified_artist_id AS (PARTITION BY artist_is_verified, artist_id)
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        duration_sec_plus_bpm,
        release_date_label,
        artist_name,
        artist_is_verified,
        duration_sec_plus_bpm_change,
        duration_sec_plus_bpm_max_7,
        artist_is_verified_artist_id_avg_duration_sec,
        artist_is_verified_artist_id_min_duration_sec_pl,
        artist_is_verified_artist_id_sum_duration_sec,
        running_duration_sec_plus_bpm,
        FLOOR(bpm / 25) * 25 AS bpm_bucket,
        duration_sec - bpm AS duration_sec_minus_bpm
    FROM with_windows
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        artist_is_verified,
        COUNT(*) AS cnt,
        MAX(duration_sec) AS max_duration_sec,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(CASE WHEN NOT is_explicit THEN 1 ELSE 0 END) AS n_not_is_explicit,
        ARRAY_SORT(COLLECT_SET(artist_name)) AS artist_name_list
    FROM derived
    GROUP BY 1
    HAVING AVG(artist_is_verified_artist_id_avg_duration_sec) > 172.5
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        n_not_is_explicit - LAG(n_not_is_explicit) OVER w_distinct_ids_n_not_is_explic AS n_not_is_explicit_change,
        ROUND(AVG(max_duration_sec) OVER (PARTITION BY artist_is_verified ORDER BY cnt ASC, artist_is_verified ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS max_duration_sec_avg_7,
        FIRST_VALUE(max_duration_sec) IGNORE NULLS OVER (PARTITION BY artist_is_verified ORDER BY cnt ASC, artist_is_verified ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_max_duration_sec,
        SUM(n_not_is_explicit) OVER (PARTITION BY artist_is_verified ORDER BY cnt ASC, artist_is_verified ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS n_not_is_explicit_sum_6_prior
    FROM totals
    WINDOW w_distinct_ids_n_not_is_explic AS (PARTITION BY distinct_ids, n_not_is_explicit ORDER BY n_not_is_explicit DESC, artist_is_verified)
)
SELECT
    artist_is_verified,
    cnt,
    max_duration_sec,
    n_not_is_explicit,
    n_not_is_explicit_change,
    first_known_max_duration_sec,
    n_not_is_explicit_sum_6_prior
FROM ranked
ORDER BY cnt NULLS LAST
