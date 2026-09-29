WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'G%'
),
with_parent AS (
    -- Join artists
    SELECT s.*, a.artist_name, a.country AS artist_country
    FROM base s
    LEFT JOIN artists a ON a.id = s.artist_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_all_by_duration_sec_ties, 3) AS duration_sec_cume_dist,
        MAX(bpm) OVER w_artist_id AS artist_id_max_bpm,
        COUNT(*) OVER (PARTITION BY artist_id) AS rows_in_artist_id,
        LAST_VALUE(release_date) OVER (PARTITION BY YEAR(release_date), artist_country ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_release_date,
        LAG(bpm) OVER (ORDER BY bpm, id) AS prev_bpm,
        LAST_VALUE(bpm) OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm,
        SUM(bpm) OVER (PARTITION BY artist_id) AS artist_id_sum_bpm,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id), 2) AS bpm_vs_artist_id_avg,
        SIZE(COLLECT_SET(artist_country) OVER (PARTITION BY artist_id)) AS distinct_artist_country_in_artist_id,
        FIRST_VALUE(release_date) OVER (ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_release_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(release_date), artist_country ORDER BY release_date DESC), 3) AS release_date_percent_rank,
        ROUND(AVG(bpm) OVER w_artist_id, 2) AS artist_id_avg_bpm,
        NTILE(3) OVER (PARTITION BY artist_country, artist_id ORDER BY bpm, id) AS bpm_tercile,
        LAST_VALUE(duration_sec) OVER (PARTITION BY YEAR(release_date), artist_country ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_sec,
        COUNT(*) OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM with_parent
    WINDOW w_all_by_duration_sec_ties AS (ORDER BY duration_sec), w_artist_id AS (PARTITION BY artist_id)
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        duration_sec,
        release_date,
        bpm,
        release_date_label,
        artist_country,
        rows_in_artist_id,
        prev_bpm,
        last_bpm,
        artist_id_sum_bpm,
        bpm_vs_artist_id_avg,
        release_date_percent_rank,
        bpm_tercile,
        CONCAT(title, '#', CAST(rows_in_artist_id AS STRING)) AS title_ref,
        ROUND(bpm_vs_artist_id_avg * 1.07, 2) AS bpm_vs_artist_id_avg_adj,
        ARRAY_CONTAINS(moods, 'focus') AS has_focus
    FROM sequenced
    WHERE artist_id_max_bpm BETWEEN 321 AND 1365
)
-- Final projection
SELECT
    id,
    duration_sec,
    release_date,
    bpm,
    release_date_label,
    artist_country,
    rows_in_artist_id,
    prev_bpm,
    last_bpm,
    bpm_tercile,
    title_ref,
    bpm_vs_artist_id_avg_adj,
    has_focus,
    rows_in_artist_id - prev_bpm AS rows_in_artist_id_minus_prev_bpm
FROM labeled
