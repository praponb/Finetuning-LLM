WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        TRUNC(release_date, 'MM') AS release_date_month_start,
        DATE_TRUNC('QUARTER', release_date) AS release_date_quarter,
        CASE WHEN duration_sec >= 353 THEN 'HIGH' WHEN duration_sec >= 183 THEN 'MEDIUM' ELSE 'LOW' END AS duration_sec_tier
    FROM tracks
    WHERE duration_sec >= 165
),
latest_per_key AS (
    -- Keep the earliest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        release_date_month_start,
        release_date_quarter,
        duration_sec_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date_month_start, id) AS rn
        FROM valid
    ) ranked
    WHERE rn = 1
)
SELECT *
FROM (
    SELECT release_date_quarter, is_explicit, duration_sec_tier, duration_sec
    FROM latest_per_key
) src
PIVOT (
    MAX(duration_sec)
    FOR duration_sec_tier IN ('MEDIUM', 'HIGH')
)
ORDER BY release_date_quarter, is_explicit
