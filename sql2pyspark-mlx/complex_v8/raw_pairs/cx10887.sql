WITH scoped_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        bpm,
        moods,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(bpm) IGNORE NULLS OVER (PARTITION BY artist_id, title_len ORDER BY title_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id), 0), 4) AS bpm_share_of_artist_id,
        FIRST_VALUE(title_len) IGNORE NULLS OVER (ORDER BY title_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_title_len,
        bpm - LAG(bpm) OVER (ORDER BY title_len, id) AS bpm_change,
        RANK() OVER (ORDER BY title_len, id) AS title_len_rank,
        ROUND(AVG(title_len) OVER (PARTITION BY artist_id, title_len ORDER BY title_len DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS title_len_avg_2_prior,
        title_len - LAG(title_len) OVER (ORDER BY title_len, id) AS title_len_change,
        MAX(title_len) OVER (PARTITION BY artist_id, title_len ORDER BY title_len, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS title_len_max_2_prior,
        ROUND(title_len / NULLIF(SUM(title_len) OVER (PARTITION BY title_len), 0), 4) AS title_len_share_of_title_len,
        LAG(bpm, 3, 0) OVER (PARTITION BY title_len, artist_id ORDER BY bpm ASC, id) AS prev3_bpm,
        ROUND(PERCENT_RANK() OVER (ORDER BY title_len), 3) AS title_len_percent_rank,
        MAX(bpm) OVER (PARTITION BY artist_id) AS artist_id_max_bpm,
        ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY title_len DESC, id) AS seq_no,
        NTILE(3) OVER (PARTITION BY artist_id, title_len ORDER BY title_len DESC, id) AS title_len_tercile,
        DENSE_RANK() OVER w_artist_id_title_len_by_title_5 AS title_len_dense_rank
    FROM scoped_tracks
    WINDOW w_artist_id_title_len_by_title_5 AS (PARTITION BY artist_id, title_len ORDER BY title_len)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        artist_id,
        title,
        bpm,
        moods,
        title_len,
        first_known_bpm,
        bpm_share_of_artist_id,
        first_known_title_len,
        bpm_change,
        title_len_rank,
        title_len_avg_2_prior,
        title_len_change,
        title_len_max_2_prior,
        title_len_share_of_title_len,
        prev3_bpm,
        title_len_percent_rank,
        artist_id_max_bpm,
        title_len_tercile,
        title_len_dense_rank
    FROM with_trends
    WHERE seq_no = 1
      AND bpm_change <= 72
)
SELECT
    id,
    artist_id,
    title,
    bpm,
    first_known_bpm,
    bpm_share_of_artist_id,
    first_known_title_len,
    title_len_rank,
    title_len_change,
    title_len_max_2_prior,
    title_len_share_of_title_len,
    prev3_bpm,
    title_len_percent_rank,
    title_len_tercile,
    title_len_dense_rank,
    bpm_change + first_known_title_len AS bpm_change_plus_first_known_title_len,
    CASE WHEN bpm_share_of_artist_id >= 0.7 THEN 'HIGH' WHEN bpm_share_of_artist_id >= 0.2 THEN 'MEDIUM' ELSE 'LOW' END AS bpm_share_of_artist_id_tier
FROM top_per_group
WHERE title_len BETWEEN 3 AND 13
ORDER BY title_len_change, title_len_tercile
