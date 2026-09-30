WITH prepared_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        release_date,
        moods,
        GREATEST(duration_sec, bpm) AS greatest_duration_sec_bpm
    FROM tracks
    WHERE bpm >= 100
      AND duration_sec > 165
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        moods,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date)) AS rows_in_release_date_month,
        LAST_VALUE(greatest_duration_sec_bpm) IGNORE NULLS OVER (ORDER BY greatest_duration_sec_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_greatest_duration_sec_bpm,
        COUNT(*) OVER (ORDER BY greatest_duration_sec_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAST_VALUE(greatest_duration_sec_bpm) IGNORE NULLS OVER (PARTITION BY YEAR(release_date), artist_id ORDER BY release_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_greatest_duration_sec_bpm_2,
        NTILE(3) OVER w_release_date_year_artist_id_2 AS release_date_tercile,
        RANK() OVER (ORDER BY greatest_duration_sec_bpm) AS greatest_duration_sec_bpm_rank,
        LAST_VALUE(greatest_duration_sec_bpm) OVER (ORDER BY greatest_duration_sec_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_greatest_duration_sec_bpm,
        MIN(greatest_duration_sec_bpm) OVER w_release_date_month AS release_date_month_min_greatest_duration_sec_bpm,
        FIRST_VALUE(release_date) IGNORE NULLS OVER (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date) ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date
    FROM prepared_tracks
    WINDOW w_release_date_year_artist_id_2 AS (PARTITION BY YEAR(release_date), artist_id ORDER BY release_date DESC NULLS FIRST, id), w_release_date_month AS (PARTITION BY DATE_TRUNC('MONTH', release_date))
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(running_count) OVER (PARTITION BY artist_id ORDER BY release_date_tercile DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS running_count_sum_2_prior,
        RANK() OVER (PARTITION BY greatest_duration_sec_bpm_rank ORDER BY greatest_duration_sec_bpm_rank DESC, id) AS greatest_duration_sec_bpm_rank_rank,
        LEAD(running_count) OVER w_greatest_duration_sec_bpm_ra AS next_running_count,
        LEAD(rows_in_release_date_month) OVER (ORDER BY rows_in_release_date_month ASC, id) AS next_rows_in_release_date_month,
        COUNT(*) OVER (PARTITION BY greatest_duration_sec_bpm_rank, release_date_tercile) AS rows_in_greatest_duration_sec_bpm_rank_release_d,
        MIN(rows_in_release_date_month) OVER (PARTITION BY greatest_duration_sec_bpm_rank, release_date_tercile) AS greatest_duration_sec_bpm_rank_release_date_terc,
        running_count - LAG(running_count) OVER (PARTITION BY artist_id ORDER BY release_date_tercile DESC NULLS FIRST, id) AS running_count_change
    FROM with_windows
    WINDOW w_greatest_duration_sec_bpm_ra AS (PARTITION BY greatest_duration_sec_bpm_rank ORDER BY greatest_duration_sec_bpm_rank DESC, id)
)
SELECT DISTINCT
    running_count_sum_2_prior,
    next_rows_in_release_date_month,
    first_known_release_date
FROM sequenced
ORDER BY first_known_release_date DESC, running_count_sum_2_prior DESC
