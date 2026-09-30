WITH base AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        release_date,
        moods,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag,
        ABS(duration_sec - 309) AS duration_sec_distance
    FROM tracks
    WHERE bpm < 165
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY is_explicit_flag ORDER BY is_explicit_flag, id) AS is_explicit_flag_decile,
        MAX(duration_sec_distance) OVER (PARTITION BY is_explicit_flag) AS is_explicit_flag_max_duration_sec_distance,
        MAX(duration_sec_distance) OVER (PARTITION BY is_explicit_flag, artist_id) AS is_explicit_flag_artist_id_max_duration_sec_dist,
        MAX(duration_sec_distance) OVER (PARTITION BY is_explicit_flag ORDER BY is_explicit_flag, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS duration_sec_distance_max_3,
        LAST_VALUE(duration_sec_distance) IGNORE NULLS OVER (PARTITION BY is_explicit_flag ORDER BY is_explicit_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec_distance,
        ROUND(is_explicit_flag - AVG(is_explicit_flag) OVER (PARTITION BY is_explicit_flag), 2) AS is_explicit_flag_vs_is_explicit_flag_avg,
        LAG(duration_sec_distance, 3) OVER w_is_explicit_flag_artist_id_b AS prev3_duration_sec_distance,
        ROUND(is_explicit_flag / NULLIF(SUM(is_explicit_flag) OVER w_is_explicit_flag_artist_id, 0), 4) AS is_explicit_flag_share_of_is_explicit_flag_artis,
        LAG(release_date, 2) OVER w_is_explicit_flag_by_is_expli AS prev2_release_date,
        DATEDIFF(release_date, LAG(release_date) OVER w_is_explicit_flag_artist_id_b) AS days_since_prev_release_date,
        ROUND(CUME_DIST() OVER w_is_explicit_flag_by_is_expli_4, 3) AS is_explicit_flag_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY is_explicit_flag ORDER BY is_explicit_flag DESC, id) AS is_explicit_flag_seq,
        ROUND(CUME_DIST() OVER w_is_explicit_flag_artist_id_b_2, 3) AS is_explicit_flag_cume_dist_2,
        LAST_VALUE(release_date) OVER (PARTITION BY artist_id ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_release_date
    FROM base
    WINDOW w_is_explicit_flag_artist_id_b AS (PARTITION BY is_explicit_flag, artist_id ORDER BY is_explicit_flag, id), w_is_explicit_flag_artist_id AS (PARTITION BY is_explicit_flag, artist_id), w_is_explicit_flag_by_is_expli AS (PARTITION BY is_explicit_flag ORDER BY is_explicit_flag, id), w_is_explicit_flag_by_is_expli_4 AS (PARTITION BY is_explicit_flag ORDER BY is_explicit_flag DESC, id), w_is_explicit_flag_artist_id_b_2 AS (PARTITION BY is_explicit_flag, artist_id ORDER BY is_explicit_flag)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        is_explicit_flag,
        is_explicit_flag_decile,
        SUM(CASE WHEN UPPER(TRIM(title)) LIKE 'G%' THEN is_explicit_flag_share_of_is_explicit_flag_artis ELSE 0 END) AS is_explicit_flag_share_of_is_explicit_flag_artis_2,
        ROUND(STDDEV(is_explicit_flag_vs_is_explicit_flag_avg), 2) AS stddev_is_explicit_flag_vs_is_explicit_flag_avg,
        ROUND(AVG(last_known_duration_sec_distance), 2) AS avg_last_known_duration_sec_distance,
        ROUND(SUM(is_explicit_flag_cume_dist_2) / NULLIF(SUM(days_since_prev_release_date), 0), 3) AS is_explicit_flag_cume_dist_2_to_days_since_prev_
    FROM with_windows
    GROUP BY is_explicit_flag, is_explicit_flag_decile
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        is_explicit_flag,
        is_explicit_flag_decile,
        is_explicit_flag_share_of_is_explicit_flag_artis_2,
        stddev_is_explicit_flag_vs_is_explicit_flag_avg,
        avg_last_known_duration_sec_distance,
        is_explicit_flag_cume_dist_2_to_days_since_prev_,
        RANK() OVER w_is_explicit_flag_decile_by_i AS is_explicit_flag_cume_dist_2_to_days_since_prev__2,
        NTILE(4) OVER (PARTITION BY is_explicit_flag_decile ORDER BY is_explicit_flag_cume_dist_2_to_days_since_prev_ DESC, is_explicit_flag, is_explicit_flag_decile) AS is_explicit_flag_cume_dist_2_to_days_since_prev__3
    FROM rollup_base
    WINDOW w_is_explicit_flag_decile_by_i AS (PARTITION BY is_explicit_flag_decile ORDER BY is_explicit_flag_cume_dist_2_to_days_since_prev_ DESC, is_explicit_flag, is_explicit_flag_decile)
)
SELECT
    is_explicit_flag,
    is_explicit_flag_decile,
    stddev_is_explicit_flag_vs_is_explicit_flag_avg,
    is_explicit_flag_cume_dist_2_to_days_since_prev_,
    is_explicit_flag_cume_dist_2_to_days_since_prev__3,
    FLOOR(is_explicit_flag_decile / 2) * 2 AS is_explicit_flag_decile_bucket,
    CASE WHEN is_explicit_flag >= 1 THEN 'A' WHEN is_explicit_flag >= 0 THEN 'B' ELSE 'C' END AS is_explicit_flag_tier
FROM ranked
