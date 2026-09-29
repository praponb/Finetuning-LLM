WITH prepared AS (
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
        ABS(bpm - 127) AS bpm_distance,
        bpm + duration_sec AS bpm_plus_duration_sec,
        DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date
    FROM tracks
    WHERE NOT is_explicit
),
tagged AS (
    -- Add calculated columns
    SELECT *, ABS(bpm_plus_duration_sec - 411) AS bpm_plus_duration_sec_distance
    FROM prepared
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM tagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND NOT p.is_skipped
              AND p.ms_played <= 214343
        )
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(bpm_plus_duration_sec) OVER (PARTITION BY is_explicit, DATE_TRUNC('MONTH', release_date) ORDER BY bpm_distance ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_bpm_plus_duration_sec,
        ROUND(AVG(bpm_plus_duration_sec) OVER (PARTITION BY is_explicit, DATE_TRUNC('MONTH', release_date) ORDER BY bpm_distance ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS bpm_plus_duration_sec_avg_3,
        bpm_plus_duration_sec - LAG(bpm_plus_duration_sec) OVER w_artist_id_by_release_date AS bpm_plus_duration_sec_change
    FROM screened
    WINDOW w_artist_id_by_release_date AS (PARTITION BY artist_id ORDER BY release_date, id)
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    is_explicit,
    moods,
    bpm_distance,
    days_since_release_date,
    first_bpm_plus_duration_sec,
    bpm_plus_duration_sec_avg_3,
    bpm_plus_duration_sec_change,
    CONCAT(title, '#', CAST(bpm_plus_duration_sec AS STRING)) AS title_ref
FROM windowed
ORDER BY bpm_plus_duration_sec_avg_3 DESC
