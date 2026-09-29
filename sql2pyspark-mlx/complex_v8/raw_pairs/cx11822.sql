WITH base_tracks AS (
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
        CONCAT(title, '#', CAST(id AS STRING)) AS title_ref,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
    WHERE bpm BETWEEN 76 AND 147
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        release_date,
        bpm,
        moods,
        title_ref,
        title_len,
        ROUND(title_len - AVG(title_len) OVER (PARTITION BY title_len), 2) AS title_len_vs_title_len_avg,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY title_len), 0), 4) AS duration_sec_share_of_title_len,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY title_len)) AS distinct_artist_id_in_title_len,
        LAST_VALUE(release_date) IGNORE NULLS OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date,
        ROUND(AVG(title_len) OVER (ORDER BY duration_sec, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS title_len_avg_3_prior,
        ROUND(AVG(duration_sec) OVER w_title_len, 2) AS title_len_avg_duration_sec,
        DENSE_RANK() OVER w_all_by_title_len AS title_len_dense_rank,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec,
        RANK() OVER (ORDER BY duration_sec) AS duration_sec_rank,
        ROUND(AVG(bpm) OVER w_title_len, 2) AS title_len_avg_bpm,
        MAX(bpm) OVER (PARTITION BY title_len) AS title_len_max_bpm,
        ROUND(CUME_DIST() OVER (ORDER BY duration_sec), 3) AS duration_sec_cume_dist,
        ROUND(duration_sec - AVG(duration_sec) OVER w_title_len, 2) AS duration_sec_vs_title_len_avg
    FROM base_tracks
    WINDOW w_title_len AS (PARTITION BY title_len), w_all_by_title_len AS (ORDER BY title_len DESC, id)
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        release_date,
        title_ref,
        title_len_vs_title_len_avg,
        duration_sec_share_of_title_len,
        title_len_avg_3_prior,
        title_len_avg_duration_sec,
        title_len_dense_rank,
        first_known_duration_sec,
        duration_sec_vs_title_len_avg,
        ROUND(title_len_avg_3_prior / NULLIF(duration_sec_cume_dist, 0), 3) AS title_len_avg_3_prior_per_duration_sec_cume_dist,
        COALESCE(title_len, 0) AS title_len_filled
    FROM sequenced
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(first_known_duration_sec, 2) OVER (ORDER BY duration_sec_vs_title_len_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_known_duration_sec,
        LAG(release_date, 2) OVER (PARTITION BY title_len_dense_rank ORDER BY duration_sec_share_of_title_len NULLS LAST, id) AS prev2_release_date,
        ROUND(AVG(title_len_filled) OVER w_title_len_filled, 2) AS title_len_filled_avg_title_len_filled
    FROM annotated
    WINDOW w_title_len_filled AS (PARTITION BY title_len_filled)
)
SELECT
    id,
    release_date,
    title_ref,
    duration_sec_share_of_title_len,
    title_len_avg_3_prior,
    title_len_avg_duration_sec,
    title_len_dense_rank,
    first_known_duration_sec,
    duration_sec_vs_title_len_avg,
    title_len_avg_3_prior_per_duration_sec_cume_dist,
    second_first_known_duration_sec,
    title_len_filled_avg_title_len_filled,
    LENGTH(TRIM(title_ref)) AS title_ref_len,
    COALESCE(title_len_filled, 0) AS title_len_filled_filled
FROM with_trends
WHERE title_ref IS NOT NULL
ORDER BY title_ref
