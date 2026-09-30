WITH filtered AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        DATE_TRUNC('YEAR', release_date) AS release_date_year
    FROM tracks
    WHERE is_explicit = TRUE
),
unique_rows AS (
    -- Keep the earliest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        release_date_year
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date_year, id) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
),
artist_id_stats AS (
    -- Avg duration_sec per artist_id
    SELECT artist_id, AVG(duration_sec) AS avg_duration_sec
    FROM unique_rows
    GROUP BY artist_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_duration_sec,
        ROUND(s.duration_sec - g.avg_duration_sec, 2) AS duration_sec_minus_avg
    FROM unique_rows s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.duration_sec > g.avg_duration_sec
),
qualified AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM outliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.bpm < 155
        )
      AND s.avg_duration_sec > 139.5
),
with_activity AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM qualified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND YEAR(t.release_date) = 2025
        )
      AND s.avg_duration_sec >= 205.5
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    release_date,
    release_date_year,
    avg_duration_sec,
    duration_sec_minus_avg
FROM with_activity
ORDER BY duration_sec_minus_avg, id DESC
