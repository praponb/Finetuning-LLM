WITH recent_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        LOWER(TRIM(title)) AS title_clean,
        GREATEST(bpm, duration_sec) AS greatest_bpm_duration_sec,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag
    FROM tracks
    WHERE is_explicit = TRUE
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(release_date) OVER (ORDER BY is_explicit_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_release_date,
        SUM(greatest_bpm_duration_sec) OVER w_artist_id_is_explicit AS artist_id_is_explicit_sum_greatest_bpm_duration_,
        is_explicit_flag - LAG(is_explicit_flag) OVER w_all_by_is_explicit_flag AS is_explicit_flag_change,
        COUNT(*) OVER (ORDER BY is_explicit_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROW_NUMBER() OVER w_all_by_is_explicit_flag AS seq_no
    FROM recent_tracks
    WINDOW w_artist_id_is_explicit AS (PARTITION BY artist_id, is_explicit), w_all_by_is_explicit_flag AS (ORDER BY is_explicit_flag, id)
),
screened AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.release_date BETWEEN DATE '2024-04-29' AND DATE '2025-04-02'
              AND t.duration_sec > 165
        )
      AND s.is_explicit_flag_change < 0
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(is_explicit_flag) OVER (ORDER BY artist_id_is_explicit_sum_greatest_bpm_duration_ DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_is_explicit_flag_so_far,
        SUM(greatest_bpm_duration_sec) OVER (ORDER BY artist_id_is_explicit_sum_greatest_bpm_duration_ DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS greatest_bpm_duration_sec_sum_1_prior,
        NTILE(10) OVER w_all_by_artist_id_is_explicit_3 AS artist_id_is_explicit_sum_greatest_bpm_duration__2
    FROM screened
    WINDOW w_all_by_artist_id_is_explicit_3 AS (ORDER BY artist_id_is_explicit_sum_greatest_bpm_duration_ DESC, id)
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(title)) AS title_len,
        ROUND(running_count / NULLIF(duration_sec, 0), 3) AS running_count_per_duration_sec,
        MONTH(release_date) AS release_date_month
    FROM ranked
    WHERE running_count < 12
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        moods,
        title_clean,
        greatest_bpm_duration_sec,
        is_explicit_flag,
        first_release_date,
        running_count,
        seq_no,
        min_is_explicit_flag_so_far,
        greatest_bpm_duration_sec_sum_1_prior,
        running_count_per_duration_sec,
        release_date_month,
        RANK() OVER (ORDER BY artist_id_is_explicit_sum_greatest_bpm_duration__2 ASC, id) AS artist_id_is_explicit_sum_greatest_bpm_duration__3,
        MAX(title_len) OVER (ORDER BY artist_id_is_explicit_sum_greatest_bpm_duration__2 ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_title_len_so_far
    FROM labeled
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    moods,
    greatest_bpm_duration_sec,
    is_explicit_flag,
    first_release_date,
    seq_no,
    greatest_bpm_duration_sec_sum_1_prior,
    running_count_per_duration_sec,
    release_date_month,
    artist_id_is_explicit_sum_greatest_bpm_duration__3
FROM metrics
