WITH filtered AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE bpm > 76
      AND release_date < DATE '2025-08-28'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(duration_sec) OVER w_is_explicit AS is_explicit_sum_duration_sec,
        MAX(bpm) OVER (PARTITION BY MONTH(release_date), artist_id ORDER BY bpm DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS bpm_max_2,
        MAX(bpm) OVER (PARTITION BY MONTH(release_date), artist_id ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_bpm_so_far,
        MAX(duration_sec) OVER (PARTITION BY is_explicit) AS is_explicit_max_duration_sec,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY is_explicit)) AS distinct_artist_id_in_is_explicit,
        SUM(bpm) OVER (PARTITION BY MONTH(release_date), artist_id ORDER BY bpm DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS bpm_sum_6_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(release_date), artist_id ORDER BY bpm DESC, id), 3) AS bpm_percent_rank,
        LEAD(bpm, 3) OVER (ORDER BY bpm DESC, id) AS next3_bpm,
        bpm - LAG(bpm) OVER (PARTITION BY is_explicit, DATE_TRUNC('MONTH', release_date) ORDER BY bpm, id) AS bpm_change,
        MAX(duration_sec) OVER (PARTITION BY MONTH(release_date), artist_id ORDER BY bpm DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS duration_sec_max_3,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY is_explicit), 2) AS bpm_vs_is_explicit_avg,
        DATEDIFF(release_date, LAG(release_date) OVER (ORDER BY bpm DESC, id)) AS days_since_prev_release_date,
        LEAD(duration_sec) OVER (ORDER BY bpm DESC, id) AS next_duration_sec,
        MAX(duration_sec) OVER (ORDER BY bpm DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS duration_sec_max_6_prior
    FROM filtered
    WINDOW w_is_explicit AS (PARTITION BY is_explicit)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        is_explicit,
        SUM(next_duration_sec) AS total_next_duration_sec,
        SUM(bpm) AS total_bpm,
        MAX(next_duration_sec) AS max_next_duration_sec
    FROM with_trends
    GROUP BY is_explicit
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        is_explicit,
        total_next_duration_sec,
        total_bpm,
        LEAD(max_next_duration_sec, 2, 0) OVER (ORDER BY total_next_duration_sec DESC, is_explicit) AS next2_max_next_duration_sec,
        ROUND(AVG(total_next_duration_sec) OVER (ORDER BY total_next_duration_sec DESC, is_explicit ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS total_next_duration_sec_avg_6_prior,
        ROW_NUMBER() OVER w_all_by_total_next_duration_s AS seq_no,
        SUM(total_next_duration_sec) OVER (ORDER BY total_next_duration_sec DESC, is_explicit ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS total_next_duration_sec_sum_2
    FROM rollup_base
    WINDOW w_all_by_total_next_duration_s AS (ORDER BY total_next_duration_sec DESC, is_explicit)
)
-- Final projection
SELECT
    is_explicit,
    total_next_duration_sec,
    total_next_duration_sec_avg_6_prior,
    seq_no,
    total_next_duration_sec_sum_2
FROM with_windows
ORDER BY total_next_duration_sec_avg_6_prior DESC NULLS FIRST, total_next_duration_sec_sum_2 DESC, is_explicit
LIMIT 5
