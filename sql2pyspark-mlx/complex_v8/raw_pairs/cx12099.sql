WITH prepared_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        CASE WHEN bpm >= 142 THEN 'A' WHEN bpm >= 76 THEN 'B' ELSE 'C' END AS bpm_tier,
        DATE_TRUNC('MONTH', release_date) AS release_date_month
    FROM tracks
    WHERE NOT is_explicit
),
bpm_tier_stats AS (
    -- Avg duration_sec per bpm_tier
    SELECT bpm_tier, AVG(duration_sec) AS avg_duration_sec
    FROM prepared_tracks
    GROUP BY bpm_tier
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_duration_sec,
        ROUND(s.duration_sec - g.avg_duration_sec, 2) AS duration_sec_minus_avg
    FROM prepared_tracks s
    JOIN bpm_tier_stats g ON g.bpm_tier = s.bpm_tier
    WHERE s.duration_sec > g.avg_duration_sec
),
combined_details_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.bpm_tier,
        s.release_date_month,
        s.avg_duration_sec,
        s.duration_sec_minus_avg,
        a.artist_name
    FROM above_average s
    JOIN artists a ON a.id = s.artist_id
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM combined_details_artist
    WHERE duration_sec_minus_avg > (SELECT AVG(duration_sec_minus_avg) FROM combined_details_artist)
),
artist_id_stats AS (
    -- Avg duration_sec per artist_id
    SELECT artist_id, AVG(duration_sec) AS avg_duration_sec_2
    FROM qualified
    GROUP BY artist_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_duration_sec_2,
        ROUND(s.duration_sec - g.avg_duration_sec_2, 2) AS duration_sec_minus_avg_2
    FROM qualified s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.duration_sec > g.avg_duration_sec_2
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE p.device IS NOT NULL) FROM plays p WHERE p.track_id = s.id) AS play_device_count,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count
    FROM compared s
)
-- Final projection
SELECT
    id,
    artist_id,
    bpm_tier,
    release_date_month,
    avg_duration_sec,
    duration_sec_minus_avg,
    avg_duration_sec_2,
    duration_sec_minus_avg_2,
    play_device_count,
    play_count,
    TO_DATE(release_date_month) AS release_date_month_date,
    COALESCE(duration_sec_minus_avg_2, 0.0) AS duration_sec_minus_avg_2_filled
FROM summarized_rows
WHERE play_count < 6
