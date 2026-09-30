WITH valid AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        moods,
        UPPER(TRIM(title)) AS title_clean,
        DATE_TRUNC('MONTH', release_date) AS release_date_month
    FROM tracks
    WHERE bpm <= 163
      AND duration_sec < 365
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(duration_sec) OVER w_artist_id AS artist_id_min_duration_sec,
        MIN(duration_sec) OVER (PARTITION BY is_explicit ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_duration_sec_so_far,
        LEAD(release_date_month, 2) OVER (PARTITION BY is_explicit ORDER BY duration_sec DESC, id) AS next2_release_date_month,
        ROUND(AVG(duration_sec) OVER (PARTITION BY is_explicit ORDER BY duration_sec DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_2
    FROM valid
    WINDOW w_artist_id AS (PARTITION BY artist_id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, metric_value
FROM (
    SELECT id, duration_sec, artist_id_min_duration_sec
    FROM with_trends
) src
UNPIVOT (
    metric_value FOR metric IN (duration_sec, artist_id_min_duration_sec)
) unp
ORDER BY id, metric
