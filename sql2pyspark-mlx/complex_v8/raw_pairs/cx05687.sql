WITH valid_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        moods,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag
    FROM tracks
    WHERE bpm BETWEEN 73 AND 155
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY is_explicit_flag)) AS distinct_artist_id_in_is_explicit_flag,
        RANK() OVER w_artist_id_by_is_explicit_fla AS is_explicit_flag_rank,
        LAST_VALUE(bpm) IGNORE NULLS OVER (PARTITION BY is_explicit_flag, artist_id ORDER BY is_explicit_flag DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_bpm,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY is_explicit_flag), 2) AS duration_sec_vs_is_explicit_flag_avg,
        COUNT(*) OVER (PARTITION BY is_explicit_flag) AS rows_in_is_explicit_flag,
        ROUND(CUME_DIST() OVER w_is_explicit_flag_by_duration, 3) AS duration_sec_cume_dist,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY artist_id), 0), 4) AS duration_sec_share_of_artist_id,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id), 2) AS artist_id_avg_bpm,
        LAST_VALUE(bpm) OVER (PARTITION BY is_explicit_flag, artist_id ORDER BY is_explicit_flag DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm,
        FIRST_VALUE(is_explicit_flag) OVER (PARTITION BY is_explicit_flag ORDER BY bpm DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_is_explicit_flag,
        FIRST_VALUE(bpm) OVER (PARTITION BY artist_id ORDER BY is_explicit_flag DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_bpm,
        SUM(duration_sec) OVER w_is_explicit_flag AS is_explicit_flag_sum_duration_sec,
        LAST_VALUE(bpm) OVER (PARTITION BY is_explicit_flag, artist_id ORDER BY bpm ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm_2
    FROM valid_tracks
    WINDOW w_artist_id_by_is_explicit_fla AS (PARTITION BY artist_id ORDER BY is_explicit_flag DESC, id), w_is_explicit_flag_by_duration AS (PARTITION BY is_explicit_flag ORDER BY duration_sec ASC), w_is_explicit_flag AS (PARTITION BY is_explicit_flag)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        moods,
        distinct_artist_id_in_is_explicit_flag,
        is_explicit_flag_rank,
        last_known_bpm,
        duration_sec_vs_is_explicit_flag_avg,
        rows_in_is_explicit_flag,
        duration_sec_cume_dist,
        duration_sec_share_of_artist_id,
        artist_id_avg_bpm,
        first_is_explicit_flag,
        is_explicit_flag_sum_duration_sec,
        MIN(last_bpm_2) OVER w_first_is_explicit_flag_disti AS first_is_explicit_flag_distinct_artist_id_in_is_,
        SUM(last_known_bpm) OVER (PARTITION BY artist_id, is_explicit_flag ORDER BY duration_sec, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS last_known_bpm_sum_2_prior,
        RANK() OVER (PARTITION BY artist_id, is_explicit_flag ORDER BY duration_sec, id) AS duration_sec_rank,
        MAX(artist_id_avg_bpm) OVER (PARTITION BY is_explicit_flag_rank, is_explicit_flag ORDER BY bpm DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS artist_id_avg_bpm_max_2_prior
    FROM metrics
    WINDOW w_first_is_explicit_flag_disti AS (PARTITION BY first_is_explicit_flag, distinct_artist_id_in_is_explicit_flag)
)
-- Final projection
SELECT
    id,
    duration_sec,
    distinct_artist_id_in_is_explicit_flag,
    last_known_bpm,
    duration_sec_vs_is_explicit_flag_avg,
    rows_in_is_explicit_flag,
    duration_sec_cume_dist,
    duration_sec_share_of_artist_id,
    last_known_bpm_sum_2_prior,
    duration_sec_rank,
    CASE WHEN duration_sec_rank >= 6 THEN 'large' WHEN duration_sec_rank >= 4 THEN 'medium' ELSE 'small' END AS duration_sec_rank_tier
FROM windowed
ORDER BY rows_in_is_explicit_flag NULLS LAST, last_known_bpm NULLS LAST
