WITH filtered_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        moods,
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
    WHERE bpm > 85
      AND artist_id <= 14
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(duration_sec_per_bpm) OVER (PARTITION BY title_len ORDER BY release_date, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_per_bpm_avg_1_prior,
        ROUND(title_len / NULLIF(SUM(title_len) OVER (PARTITION BY is_explicit), 0), 4) AS title_len_share_of_is_explicit,
        ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY duration_sec_per_bpm ASC, id) AS rn,
        ROW_NUMBER() OVER (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date) ORDER BY duration_sec_per_bpm, id) AS duration_sec_per_bpm_seq,
        NTILE(10) OVER (PARTITION BY title_len ORDER BY release_date, id) AS release_date_decile,
        DATEDIFF(release_date, LAG(release_date) OVER (PARTITION BY artist_id ORDER BY duration_sec_per_bpm ASC, id)) AS days_since_prev_release_date,
        LEAD(title_len) OVER (PARTITION BY title_len ORDER BY release_date, id) AS next_title_len,
        RANK() OVER w_title_len_by_release_date_ti AS release_date_rank
    FROM filtered_tracks
    WINDOW w_title_len_by_release_date_ti AS (PARTITION BY title_len ORDER BY release_date)
),
leaders AS (
    -- Top 2 per group
    SELECT *
    FROM metrics
    WHERE release_date_rank <= 2
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY YEAR(release_date), duration_sec_per_bpm_seq)) AS distinct_artist_id_in_release_date_year_duration,
        ROUND(CUME_DIST() OVER (PARTITION BY release_date_decile ORDER BY days_since_prev_release_date DESC, id), 3) AS days_since_prev_release_date_cume_dist,
        MAX(next_title_len) OVER (PARTITION BY release_date_decile ORDER BY days_since_prev_release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_next_title_len_so_far
    FROM leaders
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    release_date,
    is_explicit,
    title_len,
    duration_sec_per_bpm_avg_1_prior,
    title_len_share_of_is_explicit,
    rn,
    duration_sec_per_bpm_seq,
    release_date_decile,
    release_date_rank,
    distinct_artist_id_in_release_date_year_duration,
    max_next_title_len_so_far,
    SUBSTRING(TRIM(title), 1, 4) AS title_prefix
FROM with_trends
ORDER BY duration_sec_per_bpm_avg_1_prior DESC, id
LIMIT 5
