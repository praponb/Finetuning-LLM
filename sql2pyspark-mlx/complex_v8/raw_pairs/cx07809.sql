WITH scoped AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket,
        DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'workout')
      AND bpm BETWEEN 89 AND 165
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        days_since_release_date,
        ROUND(duration_sec_bucket / NULLIF(SUM(duration_sec_bucket) OVER (PARTITION BY YEAR(release_date), artist_id), 0), 4) AS duration_sec_bucket_share_of_release_date_year_a,
        RANK() OVER (PARTITION BY is_explicit ORDER BY release_date DESC) AS release_date_rank,
        FIRST_VALUE(duration_sec_bucket) OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec_bucket,
        ROUND(days_since_release_date - AVG(days_since_release_date) OVER (PARTITION BY YEAR(release_date), artist_id), 2) AS days_since_release_date_vs_release_date_year_art,
        COUNT(*) OVER w_artist_id_is_explicit AS rows_in_artist_id_is_explicit,
        SUM(days_since_release_date) OVER (PARTITION BY artist_id, is_explicit) AS artist_id_is_explicit_sum_days_since_release_dat,
        LEAD(duration_sec, 2, 0) OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec_bucket ASC, id) AS next2_duration_sec,
        FIRST_VALUE(duration_sec) OVER (PARTITION BY artist_id, is_explicit ORDER BY days_since_release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec,
        MAX(days_since_release_date) OVER w_release_date_year_artist_id AS release_date_year_artist_id_max_days_since_relea,
        ROUND(duration_sec_bucket - AVG(duration_sec_bucket) OVER (PARTITION BY artist_id, is_explicit), 2) AS duration_sec_bucket_vs_artist_id_is_explicit_avg
    FROM scoped
    WINDOW w_artist_id_is_explicit AS (PARTITION BY artist_id, is_explicit), w_release_date_year_artist_id AS (PARTITION BY YEAR(release_date), artist_id)
),
leaders AS (
    -- Top 3 per group
    SELECT *
    FROM metrics
    WHERE release_date_rank <= 3
)
SELECT
    id,
    title,
    release_date,
    is_explicit,
    duration_sec_bucket_share_of_release_date_year_a,
    first_duration_sec_bucket,
    days_since_release_date_vs_release_date_year_art,
    rows_in_artist_id_is_explicit,
    artist_id_is_explicit_sum_days_since_release_dat,
    next2_duration_sec,
    first_duration_sec,
    release_date_year_artist_id_max_days_since_relea,
    duration_sec_bucket_vs_artist_id_is_explicit_avg,
    ROUND(release_date_rank / NULLIF(next2_duration_sec, 0), 3) AS release_date_rank_per_next2_duration_sec
FROM leaders
WHERE NOT is_explicit
