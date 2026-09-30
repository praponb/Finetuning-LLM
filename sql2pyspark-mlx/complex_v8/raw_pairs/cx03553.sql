WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        bpm,
        MONTH(release_date) AS release_date_month,
        DATE_TRUNC('YEAR', release_date) AS release_date_year,
        ABS(duration_sec - 352) AS duration_sec_distance
    FROM tracks
    WHERE (duration_sec < 352 OR artist_id IS NOT NULL)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(bpm) OVER (PARTITION BY artist_id, release_date_month ORDER BY bpm NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_bpm,
        COUNT(*) OVER (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date)) AS rows_in_artist_id_release_date_month
    FROM recent
    WINDOW w_artist_id_release_date_month_2 AS (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date))
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN duration_sec_distance >= 252 THEN 'A' WHEN duration_sec_distance >= 189 THEN 'B' ELSE 'C' END AS duration_sec_distance_tier
    FROM sequenced
    WHERE rows_in_artist_id_release_date_month < 17
),
artist_id_stats AS (
    -- Avg first_bpm per artist_id
    SELECT artist_id, AVG(first_bpm) AS avg_first_bpm
    FROM enriched
    GROUP BY artist_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_first_bpm,
        ROUND(s.first_bpm - g.avg_first_bpm, 2) AS first_bpm_minus_avg
    FROM enriched s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.first_bpm > g.avg_first_bpm
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, value
FROM (
    SELECT id, rows_in_artist_id_release_date_month, first_bpm, duration_sec_distance, bpm
    FROM above_average
) src
UNPIVOT (
    value FOR metric_name IN (rows_in_artist_id_release_date_month AS rows_in_artist_id_release_date_month_v, first_bpm AS first_bpm_v, duration_sec_distance AS duration_sec_distance_v, bpm AS bpm_v)
) unp
