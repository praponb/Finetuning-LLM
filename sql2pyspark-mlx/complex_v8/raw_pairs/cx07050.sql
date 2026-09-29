WITH filtered_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        LENGTH(TRIM(title)) AS title_len,
        LEAST(duration_sec, bpm) AS least_duration_sec_bpm
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'P%'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(4) OVER (ORDER BY bpm DESC, id) AS bpm_quartile,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY title_len, artist_id), 0), 4) AS bpm_share_of_title_len_artist_id,
        NTH_VALUE(least_duration_sec_bpm, 2) OVER (ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_least_duration_sec_bpm,
        ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY bpm DESC, id) AS rn,
        FIRST_VALUE(bpm) IGNORE NULLS OVER (PARTITION BY title_len ORDER BY title_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm,
        ROUND(PERCENT_RANK() OVER (ORDER BY bpm DESC, id), 3) AS bpm_percent_rank,
        LAST_VALUE(title_len) OVER (ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_title_len,
        ROUND(AVG(title_len) OVER (ORDER BY bpm DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS title_len_avg_2,
        COUNT(*) OVER w_title_len_artist_id AS rows_in_title_len_artist_id,
        least_duration_sec_bpm - LAG(least_duration_sec_bpm) OVER (PARTITION BY title_len ORDER BY title_len DESC, id) AS least_duration_sec_bpm_change
    FROM filtered_tracks
    WINDOW w_title_len_artist_id AS (PARTITION BY title_len, artist_id)
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN title_len >= 11 THEN 'large' WHEN title_len >= 7 THEN 'medium' ELSE 'small' END AS title_len_tier,
        COALESCE(title_len, 0) AS title_len_filled
    FROM windowed
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_all_by_bpm_2, 3) AS bpm_percent_rank_2,
        RANK() OVER (PARTITION BY title_len_filled ORDER BY rows_in_title_len_artist_id DESC, id) AS rows_in_title_len_artist_id_rank,
        SUM(bpm_percent_rank) OVER w_last_title_len_is_explicit AS last_title_len_is_explicit_sum_bpm_percent_rank,
        COUNT(*) OVER (PARTITION BY last_title_len, is_explicit) AS rows_in_last_title_len_is_explicit,
        DENSE_RANK() OVER (PARTITION BY title_len_filled ORDER BY rows_in_title_len_artist_id DESC, id) AS rows_in_title_len_artist_id_dense_rank,
        FIRST_VALUE(rows_in_title_len_artist_id) OVER (PARTITION BY title_len_filled ORDER BY rows_in_title_len_artist_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_rows_in_title_len_artist_id,
        FIRST_VALUE(title_len_filled) OVER (ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_title_len_filled
    FROM scored
    WINDOW w_all_by_bpm_2 AS (ORDER BY bpm, id), w_last_title_len_is_explicit AS (PARTITION BY last_title_len, is_explicit)
),
summary AS (
    -- Summarise per group
    SELECT bpm_quartile, MAX(last_title_len) AS max_last_title_len, COUNT(*) AS cnt
    FROM ranked
    WHERE title_len_tier = 'large'
    GROUP BY bpm_quartile
)
SELECT
    bpm_quartile,
    max_last_title_len,
    GREATEST(cnt, max_last_title_len) AS greatest_cnt_max_last_title_len,
    ABS(cnt - 8) AS cnt_distance
FROM summary
ORDER BY max_last_title_len
