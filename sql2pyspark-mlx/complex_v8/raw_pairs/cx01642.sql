WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        duration_sec - bpm AS duration_sec_minus_bpm,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM tracks
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        duration_sec,
        release_date,
        duration_sec_filled,
        SUM(duration_sec_filled) OVER (ORDER BY release_date DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS duration_sec_filled_sum_2,
        COUNT(*) OVER w_artist_id AS rows_in_artist_id,
        FIRST_VALUE(duration_sec_minus_bpm) OVER (ORDER BY duration_sec_filled DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec_minus_bpm,
        LAST_VALUE(duration_sec) OVER (ORDER BY duration_sec_filled DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_sec,
        RANK() OVER (PARTITION BY YEAR(release_date) ORDER BY duration_sec ASC) AS duration_sec_rank,
        NTH_VALUE(duration_sec_filled, 2) OVER (ORDER BY duration_sec_filled DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec_filled,
        ROUND(duration_sec_minus_bpm / NULLIF(SUM(duration_sec_minus_bpm) OVER (PARTITION BY artist_id), 0), 4) AS duration_sec_minus_bpm_share_of_artist_id,
        ROUND(duration_sec_filled - AVG(duration_sec_filled) OVER w_artist_id, 2) AS duration_sec_filled_vs_artist_id_avg,
        COUNT(*) OVER (ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM scoped
    WINDOW w_artist_id AS (PARTITION BY artist_id)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY duration_sec_rank) AS rows_in_duration_sec_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY duration_sec_rank ORDER BY duration_sec_filled_vs_artist_id_avg), 3) AS duration_sec_filled_vs_artist_id_avg_percent_ran,
        MAX(duration_sec) OVER w_release_date_month AS release_date_month_max_duration_sec,
        ROUND(duration_sec - AVG(duration_sec) OVER w_release_date_month, 2) AS duration_sec_vs_release_date_month_avg
    FROM windowed
    WINDOW w_release_date_month AS (PARTITION BY MONTH(release_date))
)
SELECT
    id,
    release_date,
    duration_sec_filled,
    duration_sec_filled_sum_2,
    rows_in_artist_id,
    first_duration_sec_minus_bpm,
    duration_sec_rank,
    second_duration_sec_filled,
    duration_sec_minus_bpm_share_of_artist_id,
    duration_sec_filled_vs_artist_id_avg,
    duration_sec_filled_vs_artist_id_avg_percent_ran,
    release_date_month_max_duration_sec,
    YEAR(release_date) AS release_date_year
FROM ranked
WHERE duration_sec_vs_release_date_month_avg <= 210.0
ORDER BY duration_sec_filled_vs_artist_id_avg
