WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATEDIFF(DATE '2025-06-30', release_date) AS days_since_release_date,
        CONCAT(title, '#', CAST(duration_sec AS STRING)) AS title_ref
    FROM tracks
    WHERE duration_sec <= 352
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(days_since_release_date) OVER (ORDER BY duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_release_date,
        LAST_VALUE(release_date) IGNORE NULLS OVER (ORDER BY duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_id), 2) AS artist_id_avg_duration_sec,
        COUNT(*) OVER (PARTITION BY MONTH(release_date) ORDER BY bpm ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        COUNT(*) OVER (PARTITION BY artist_id) AS rows_in_artist_id,
        LAST_VALUE(duration_sec) OVER (ORDER BY days_since_release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_sec,
        ROUND(PERCENT_RANK() OVER (ORDER BY duration_sec ASC, id), 3) AS duration_sec_percent_rank,
        MAX(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_max_duration_sec,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (ORDER BY duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec,
        NTILE(4) OVER (ORDER BY days_since_release_date, id) AS days_since_release_date_quartile,
        ROUND(AVG(bpm) OVER w_artist_id, 2) AS artist_id_avg_bpm,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id), 2) AS bpm_vs_artist_id_avg,
        days_since_release_date - LAG(days_since_release_date) OVER (ORDER BY days_since_release_date, id) AS days_since_release_date_change
    FROM recent
    WINDOW w_artist_id AS (PARTITION BY artist_id)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(3) OVER w_last_known_release_date_year AS bpm_tercile,
        MAX(days_since_release_date) OVER (ORDER BY days_since_release_date DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS days_since_release_date_max_7,
        LEAD(running_count) OVER (PARTITION BY YEAR(last_known_release_date) ORDER BY bpm ASC, id) AS next_running_count,
        ROUND(days_since_release_date - AVG(days_since_release_date) OVER (PARTITION BY YEAR(release_date)), 2) AS days_since_release_date_vs_release_date_year_avg
    FROM metrics
    WINDOW w_last_known_release_date_year AS (PARTITION BY YEAR(last_known_release_date) ORDER BY bpm ASC, id)
)
SELECT
    id,
    artist_id,
    title,
    release_date,
    bpm,
    days_since_release_date,
    title_ref,
    running_days_since_release_date,
    last_known_release_date,
    artist_id_avg_duration_sec,
    running_count,
    rows_in_artist_id,
    last_duration_sec,
    duration_sec_percent_rank,
    first_known_duration_sec,
    days_since_release_date_quartile,
    bpm_vs_artist_id_avg,
    days_since_release_date_change,
    bpm_tercile,
    days_since_release_date_max_7,
    next_running_count,
    days_since_release_date_vs_release_date_year_avg,
    ROUND(next_running_count / NULLIF(rows_in_artist_id, 0), 3) AS next_running_count_per_rows_in_artist_id
FROM windowed
ORDER BY days_since_release_date_max_7, release_date DESC, id
LIMIT 10
