WITH recent_tracks AS (
    -- Keep valid rows and derive helper columns
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
    WHERE bpm BETWEEN 73 AND 156
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_sum_duration_sec,
        LAST_VALUE(release_date) IGNORE NULLS OVER (PARTITION BY artist_id ORDER BY bpm ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date,
        COUNT(*) OVER (PARTITION BY is_explicit) AS rows_in_is_explicit,
        LAST_VALUE(duration_sec) OVER (PARTITION BY artist_id, is_explicit ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_sec,
        COUNT(*) OVER (PARTITION BY artist_id) AS rows_in_artist_id,
        MAX(bpm) OVER (PARTITION BY artist_id) AS artist_id_max_bpm,
        RANK() OVER (PARTITION BY artist_id ORDER BY bpm ASC) AS bpm_rank,
        MAX(duration_sec) OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_duration_sec_so_far,
        SUM(duration_sec) OVER (PARTITION BY artist_id ORDER BY bpm ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS duration_sec_sum_1_prior
    FROM recent_tracks
),
leaders AS (
    -- Top 2 per group
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        artist_id_sum_duration_sec,
        last_known_release_date,
        rows_in_is_explicit,
        last_duration_sec,
        rows_in_artist_id,
        artist_id_max_bpm,
        max_duration_sec_so_far,
        duration_sec_sum_1_prior
    FROM sequenced
    WHERE bpm_rank <= 2
),
sequenced_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        artist_id_sum_duration_sec,
        last_known_release_date,
        last_duration_sec,
        rows_in_artist_id,
        max_duration_sec_so_far,
        duration_sec_sum_1_prior,
        ROUND(AVG(duration_sec_sum_1_prior) OVER (PARTITION BY MONTH(last_known_release_date), is_explicit ORDER BY duration_sec ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_sum_1_prior_avg_6_prior,
        FIRST_VALUE(duration_sec_sum_1_prior) OVER (PARTITION BY is_explicit ORDER BY last_known_release_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec_sum_1_prior,
        MAX(rows_in_is_explicit) OVER (ORDER BY duration_sec_sum_1_prior DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rows_in_is_explicit_max_3,
        LAG(rows_in_artist_id) OVER (ORDER BY duration_sec_sum_1_prior DESC NULLS FIRST, id) AS prev_rows_in_artist_id,
        MAX(last_duration_sec) OVER w_release_date_month AS release_date_month_max_last_duration_sec,
        SIZE(COLLECT_SET(artist_id) OVER w_release_date_month) AS distinct_artist_id_in_release_date_month
    FROM leaders
    WINDOW w_release_date_month AS (PARTITION BY DATE_TRUNC('MONTH', release_date))
)
SELECT
    id,
    artist_id,
    title,
    release_date,
    is_explicit,
    artist_id_sum_duration_sec,
    last_known_release_date,
    rows_in_artist_id,
    duration_sec_sum_1_prior,
    first_duration_sec_sum_1_prior,
    rows_in_is_explicit_max_3,
    release_date_month_max_last_duration_sec,
    distinct_artist_id_in_release_date_month,
    DATE_TRUNC('MONTH', release_date) AS release_date_month
FROM sequenced_2
ORDER BY title
