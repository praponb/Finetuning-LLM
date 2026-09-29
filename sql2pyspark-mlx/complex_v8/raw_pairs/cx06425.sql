WITH recent_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        CASE WHEN duration_sec >= 352 THEN 'large' WHEN duration_sec >= 183 THEN 'medium' ELSE 'small' END AS duration_sec_tier,
        YEAR(release_date) AS release_date_year
    FROM tracks
    WHERE duration_sec <= 357
),
artist_id_stats AS (
    -- Avg bpm per artist_id
    SELECT artist_id, AVG(bpm) AS avg_bpm
    FROM recent_tracks
    GROUP BY artist_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_bpm, ROUND(s.bpm - g.avg_bpm, 2) AS bpm_minus_avg
    FROM recent_tracks s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm > g.avg_bpm
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(duration_sec) OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm_minus_avg DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS duration_sec_max_6_prior,
        MAX(bpm_minus_avg) OVER (ORDER BY duration_sec ASC NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS bpm_minus_avg_max_7,
        FIRST_VALUE(duration_sec) OVER (ORDER BY avg_bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm_minus_avg DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_3_prior,
        ROUND(PERCENT_RANK() OVER w_all_by_avg_bpm, 3) AS avg_bpm_percent_rank
    FROM above_average
    WINDOW w_all_by_avg_bpm AS (ORDER BY avg_bpm DESC, id)
)
-- Subtotals with CUBE
SELECT
    artist_id,
    duration_sec_tier,
    release_date_year,
    MAX(bpm_minus_avg) AS max_bpm_minus_avg
FROM ranked
GROUP BY CUBE(artist_id, duration_sec_tier, release_date_year)
ORDER BY artist_id NULLS LAST, duration_sec_tier NULLS LAST, release_date_year NULLS LAST
