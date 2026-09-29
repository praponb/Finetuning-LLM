WITH recent AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        moods,
        DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag
    FROM tracks
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(release_date) OVER (PARTITION BY artist_id ORDER BY release_date DESC, id) AS next_release_date,
        COUNT(*) OVER (PARTITION BY is_explicit_flag, artist_id ORDER BY is_explicit_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER w_artist_id_is_explicit_flag, 0), 4) AS duration_sec_share_of_artist_id_is_explicit_flag,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id ORDER BY release_date DESC, id), 3) AS release_date_cume_dist,
        ROUND(AVG(duration_sec) OVER (ORDER BY days_since_release_date, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_2,
        DENSE_RANK() OVER (ORDER BY days_since_release_date) AS days_since_release_date_dense_rank,
        COUNT(*) OVER w_artist_id AS rows_in_artist_id,
        ROUND(AVG(duration_sec) OVER (ORDER BY days_since_release_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_7,
        LEAD(release_date, 3) OVER (ORDER BY days_since_release_date DESC, id) AS next3_release_date,
        LAG(is_explicit_flag, 3, 0) OVER (ORDER BY days_since_release_date DESC, id) AS prev3_is_explicit_flag,
        LAST_VALUE(duration_sec) IGNORE NULLS OVER (ORDER BY days_since_release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec
    FROM recent
    WINDOW w_artist_id_is_explicit_flag AS (PARTITION BY artist_id, is_explicit_flag), w_artist_id AS (PARTITION BY artist_id)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        title,
        release_date,
        moods,
        days_since_release_date,
        is_explicit_flag,
        next_release_date,
        running_count,
        duration_sec_share_of_artist_id_is_explicit_flag,
        release_date_cume_dist,
        days_since_release_date_dense_rank,
        rows_in_artist_id,
        prev3_is_explicit_flag,
        MIN(duration_sec_avg_2) OVER (PARTITION BY YEAR(next3_release_date), MONTH(next_release_date)) AS next3_release_date_year_next_release_date_month_,
        ROUND(CUME_DIST() OVER (PARTITION BY prev3_is_explicit_flag, is_explicit_flag ORDER BY prev3_is_explicit_flag), 3) AS prev3_is_explicit_flag_cume_dist,
        ROUND(AVG(duration_sec) OVER (PARTITION BY days_since_release_date_dense_rank ORDER BY next3_release_date DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_avg_6_prior
    FROM windowed
)
-- Final projection
SELECT
    id,
    title,
    release_date,
    moods,
    days_since_release_date,
    is_explicit_flag,
    next_release_date,
    running_count,
    days_since_release_date_dense_rank,
    rows_in_artist_id,
    prev3_is_explicit_flag_cume_dist,
    duration_sec_avg_6_prior
FROM metrics
ORDER BY id, title DESC
