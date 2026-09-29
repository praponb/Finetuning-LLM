WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        UPPER(TRIM(device)) AS device_clean,
        SUBSTRING(TRIM(device), 1, 2) AS device_prefix,
        DATE_TRUNC('HOUR', played_at) AS played_at_hour
    FROM plays
    WHERE source IS NOT NULL
      AND device IN ('tv', 'speaker', 'car')
),
without_matches AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM recent s
    WHERE s.track_id IN (
            SELECT p.track_id
            FROM plays p
        )
      AND s.source = 'search'
),
with_details_listener AS (
    -- Join listeners
    SELECT s.*, l.country AS listener_country
    FROM without_matches s
    JOIN listeners l ON l.id = s.listener_id
    WHERE l.country IN ('JP', 'KR')
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(ms_played) OVER (PARTITION BY track_id, MONTH(played_at)) AS track_id_played_at_month_sum_ms_played,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_id, track_id ORDER BY played_at ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_7
    FROM with_details_listener
)
SELECT *
FROM (
    SELECT played_at_hour, track_id, device, ms_played_avg_7
    FROM windowed
) src
PIVOT (
    SUM(ms_played_avg_7) AS amount, COUNT(ms_played_avg_7) AS n
    FOR device IN ('desktop', 'tv')
)
ORDER BY played_at_hour, track_id
