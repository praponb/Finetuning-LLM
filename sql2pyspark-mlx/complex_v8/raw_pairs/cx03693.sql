WITH valid AS (
    -- Filter tracks
    SELECT *
    FROM tracks
    WHERE LOWER(title) LIKE '%ou%'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY is_explicit, artist_id ORDER BY bpm ASC) AS bpm_rank,
        NTH_VALUE(duration_sec, 2) OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        ROUND(AVG(bpm) OVER (PARTITION BY is_explicit ORDER BY release_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_3,
        NTILE(10) OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date, id) AS release_date_decile,
        LAG(duration_sec, 2) OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date, id) AS prev2_duration_sec,
        SUM(bpm) OVER (PARTITION BY MONTH(release_date), is_explicit) AS release_date_month_is_explicit_sum_bpm,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id, is_explicit ORDER BY release_date DESC, id), 3) AS release_date_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date), 3) AS release_date_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date, id) AS rn,
        FIRST_VALUE(duration_sec) OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec,
        SUM(bpm) OVER (PARTITION BY is_explicit, artist_id ORDER BY bpm ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm,
        SUM(bpm) OVER (PARTITION BY artist_id) AS artist_id_sum_bpm
    FROM valid
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        title,
        duration_sec,
        is_explicit,
        moods,
        bpm_rank,
        second_duration_sec,
        bpm_avg_3,
        release_date_decile,
        prev2_duration_sec,
        release_date_month_is_explicit_sum_bpm,
        release_date_percent_rank,
        rn,
        first_duration_sec,
        running_bpm,
        artist_id_sum_bpm,
        ADD_MONTHS(release_date, 12) AS release_date_plus_12m
    FROM ranked
)
-- Final projection
SELECT
    id,
    title,
    duration_sec,
    moods,
    bpm_rank,
    second_duration_sec,
    bpm_avg_3,
    release_date_month_is_explicit_sum_bpm,
    release_date_percent_rank,
    running_bpm,
    release_date_plus_12m,
    SUBSTRING(TRIM(title), 1, 4) AS title_prefix
FROM scored
ORDER BY bpm_rank, release_date_percent_rank DESC
