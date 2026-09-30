WITH cleaned_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        YEAR(release_date) AS release_date_year
    FROM tracks
    WHERE is_explicit = TRUE
      AND UPPER(TRIM(title)) LIKE 'M%'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY is_explicit ORDER BY release_date_year DESC), 3) AS release_date_year_cume_dist,
        SUM(bpm) OVER (ORDER BY bpm ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm,
        LEAD(release_date) OVER (ORDER BY duration_sec, id) AS next_release_date,
        LEAD(release_date) OVER (PARTITION BY is_explicit ORDER BY release_date_year DESC, id) AS next_release_date_2,
        NTILE(10) OVER w_release_date_year_is_explici AS duration_sec_decile,
        LAG(release_date, 3) OVER (ORDER BY bpm ASC, id) AS prev3_release_date,
        LAG(release_date, 3) OVER (PARTITION BY is_explicit ORDER BY release_date_year DESC, id) AS prev3_release_date_2,
        NTILE(3) OVER (PARTITION BY release_date_year, is_explicit ORDER BY duration_sec DESC NULLS FIRST, id) AS duration_sec_tercile
    FROM cleaned_tracks
    WINDOW w_release_date_year_is_explici AS (PARTITION BY release_date_year, is_explicit ORDER BY duration_sec DESC NULLS FIRST, id)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(5) OVER (ORDER BY prev3_release_date_2 DESC, id) AS prev3_release_date_2_quintile,
        NTH_VALUE(prev3_release_date, 2) OVER (PARTITION BY duration_sec_decile ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prev3_release_date,
        DATEDIFF(next_release_date_2, LAG(next_release_date_2) OVER (PARTITION BY is_explicit ORDER BY release_date_year_cume_dist NULLS LAST, id)) AS days_since_prev_next_release_date_2,
        ROUND(AVG(running_bpm) OVER (PARTITION BY is_explicit ORDER BY release_date_year_cume_dist NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS running_bpm_avg_6_prior,
        LAG(duration_sec_decile, 3, 0) OVER w_all_by_prev3_release_date_2 AS prev3_duration_sec_decile,
        RANK() OVER (PARTITION BY is_explicit ORDER BY release_date_year_cume_dist NULLS LAST) AS release_date_year_cume_dist_rank
    FROM ranked
    WINDOW w_all_by_prev3_release_date_2 AS (ORDER BY prev3_release_date_2 DESC, id)
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    release_date,
    is_explicit,
    bpm,
    release_date_year,
    running_bpm,
    next_release_date,
    duration_sec_decile,
    prev3_release_date,
    prev3_release_date_2,
    duration_sec_tercile,
    second_prev3_release_date,
    days_since_prev_next_release_date_2,
    prev3_duration_sec_decile,
    FLOOR(bpm / 25) * 25 AS bpm_bucket
FROM with_windows
ORDER BY duration_sec_decile, release_date_year
