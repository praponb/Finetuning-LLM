WITH scoped AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        FLOOR(bpm / 25) * 25 AS bpm_bucket,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label
    FROM tracks
    WHERE NOT is_explicit
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(duration_sec) OVER w_is_explicit_by_duration_sec AS next_duration_sec,
        DATEDIFF(release_date, LAG(release_date) OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id)) AS days_since_prev_release_date,
        ROUND(AVG(bpm_bucket) OVER (PARTITION BY artist_id, is_explicit), 2) AS artist_id_is_explicit_avg_bpm_bucket,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_id, is_explicit), 2) AS duration_sec_vs_artist_id_is_explicit_avg,
        LEAD(bpm_bucket) OVER w_all_by_duration_sec AS next_bpm_bucket,
        NTILE(5) OVER (PARTITION BY is_explicit ORDER BY duration_sec NULLS LAST, id) AS duration_sec_quintile,
        ROUND(PERCENT_RANK() OVER (ORDER BY release_date ASC NULLS LAST, id), 3) AS release_date_percent_rank,
        COUNT(*) OVER (PARTITION BY artist_id) AS rows_in_artist_id
    FROM scoped
    WINDOW w_is_explicit_by_duration_sec AS (PARTITION BY is_explicit ORDER BY duration_sec NULLS LAST, id), w_all_by_duration_sec AS (ORDER BY duration_sec DESC, id), w_artist_id_is_explicit AS (PARTITION BY artist_id, is_explicit)
)
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    bpm_bucket,
    release_date_label,
    days_since_prev_release_date,
    artist_id_is_explicit_avg_bpm_bucket,
    duration_sec_vs_artist_id_is_explicit_avg,
    next_bpm_bucket,
    duration_sec_quintile,
    release_date_percent_rank,
    rows_in_artist_id,
    DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date,
    CASE WHEN rows_in_artist_id >= 14 THEN 'HIGH' WHEN rows_in_artist_id >= 6 THEN 'MEDIUM' ELSE 'LOW' END AS rows_in_artist_id_tier
FROM ranked
ORDER BY release_date DESC NULLS FIRST, rows_in_artist_id
