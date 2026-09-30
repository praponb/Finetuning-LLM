WITH scoped_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        LEAST(bpm, duration_sec) AS least_bpm_duration_sec,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket
    FROM tracks
    WHERE release_date < DATE '2025-08-28'
      AND ARRAY_CONTAINS(moods, 'party')
),
artist_id_stats AS (
    -- Avg least_bpm_duration_sec per artist_id
    SELECT artist_id, AVG(least_bpm_duration_sec) AS avg_least_bpm_duration_sec
    FROM scoped_tracks
    GROUP BY artist_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_least_bpm_duration_sec,
        ROUND(s.least_bpm_duration_sec - g.avg_least_bpm_duration_sec, 2) AS least_bpm_duration_sec_minus_avg
    FROM scoped_tracks s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.least_bpm_duration_sec > g.avg_least_bpm_duration_sec
),
artist_id_stats_2 AS (
    -- Avg least_bpm_duration_sec_minus_avg per artist_id
    SELECT artist_id, AVG(least_bpm_duration_sec_minus_avg) AS avg_least_bpm_duration_sec_minus_avg
    FROM vs_group
    GROUP BY artist_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_least_bpm_duration_sec_minus_avg,
        ROUND(s.least_bpm_duration_sec_minus_avg - g.avg_least_bpm_duration_sec_minus_avg, 2) AS least_bpm_duration_sec_minus_avg_minus_avg
    FROM vs_group s
    JOIN artist_id_stats_2 g ON g.artist_id = s.artist_id
    WHERE s.least_bpm_duration_sec_minus_avg > g.avg_least_bpm_duration_sec_minus_avg
)
SELECT id, measure, reading
FROM (
    SELECT id, duration_sec_bucket, duration_sec
    FROM outliers
) src
UNPIVOT (
    reading FOR measure IN (duration_sec_bucket, duration_sec)
) unp
ORDER BY id, measure
