WITH scoped AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix
    FROM tracks
    WHERE SIZE(moods) >= 1
      AND bpm >= 85
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(bpm) IGNORE NULLS OVER (ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm,
        LAG(release_date) OVER (PARTITION BY artist_id ORDER BY release_date DESC, id) AS prev_release_date,
        LAST_VALUE(bpm) OVER (ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm,
        ROUND(PERCENT_RANK() OVER (ORDER BY release_date DESC), 3) AS release_date_percent_rank,
        LEAD(release_date, 2) OVER (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date) ORDER BY duration_sec, id) AS next2_release_date,
        ROUND(CUME_DIST() OVER (ORDER BY duration_sec), 3) AS duration_sec_cume_dist,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY is_explicit, artist_id), 2) AS duration_sec_vs_is_explicit_artist_id_avg,
        ROUND(bpm - AVG(bpm) OVER w_artist_id, 2) AS bpm_vs_artist_id_avg,
        ROUND(AVG(duration_sec) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), is_explicit ORDER BY duration_sec ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_2,
        NTILE(10) OVER (PARTITION BY is_explicit ORDER BY bpm DESC, id) AS bpm_decile,
        FIRST_VALUE(duration_sec) OVER (PARTITION BY artist_id ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec,
        COUNT(*) OVER (PARTITION BY is_explicit, artist_id) AS rows_in_is_explicit_artist_id,
        RANK() OVER w_all_by_release_date AS release_date_rank,
        LEAD(bpm, 3, 0) OVER (ORDER BY release_date DESC, id) AS next3_bpm
    FROM scoped
    WINDOW w_artist_id AS (PARTITION BY artist_id), w_all_by_release_date AS (ORDER BY release_date DESC, id)
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN release_date_rank >= 8 THEN 'large' WHEN release_date_rank >= 5 THEN 'medium' ELSE 'small' END AS release_date_rank_tier,
        DATE_FORMAT(prev_release_date, 'yyyy-MM-dd') AS prev_release_date_label,
        DATE_FORMAT(prev_release_date, 'yyyy-MM') AS prev_release_date_label_2
    FROM metrics
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(bpm) OVER (ORDER BY first_duration_sec, id) AS next_bpm,
        SUM(first_duration_sec) OVER (PARTITION BY DATE_TRUNC('MONTH', next2_release_date), is_explicit) AS next2_release_date_month_is_explicit_sum_first_d,
        ROUND(PERCENT_RANK() OVER w_all_by_first_duration_sec_ti, 3) AS first_duration_sec_percent_rank,
        NTILE(3) OVER (ORDER BY first_duration_sec, id) AS first_duration_sec_tercile,
        MAX(first_duration_sec) OVER (PARTITION BY bpm_decile ORDER BY bpm_decile DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS first_duration_sec_max_7,
        MAX(next3_bpm) OVER (PARTITION BY DATE_TRUNC('MONTH', next2_release_date) ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_next3_bpm_so_far,
        LAG(last_bpm, 2) OVER w_bpm_decile_by_bpm_decile AS prev2_last_bpm,
        LEAD(release_date) OVER w_bpm_decile_by_bpm_decile AS next_release_date
    FROM scored
    WINDOW w_all_by_first_duration_sec_ti AS (ORDER BY first_duration_sec), w_bpm_decile_by_bpm_decile AS (PARTITION BY bpm_decile ORDER BY bpm_decile DESC, id)
)
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    is_explicit,
    bpm,
    moods,
    title_prefix,
    first_known_bpm,
    last_bpm,
    release_date_percent_rank,
    duration_sec_cume_dist,
    duration_sec_vs_is_explicit_artist_id_avg,
    bpm_vs_artist_id_avg,
    duration_sec_avg_2,
    bpm_decile,
    first_duration_sec,
    rows_in_is_explicit_artist_id,
    release_date_rank,
    prev_release_date_label,
    prev_release_date_label_2,
    next2_release_date_month_is_explicit_sum_first_d,
    first_duration_sec_percent_rank,
    first_duration_sec_tercile,
    max_next3_bpm_so_far,
    prev2_last_bpm,
    next_release_date
FROM with_windows
ORDER BY bpm DESC, id DESC
