WITH filtered_web_events AS (
    SELECT *, COALESCE(page, 'Unknown') AS page_filled
    FROM web_events
    WHERE duration_sec BETWEEN 268 AND 465
),
summary AS (
    SELECT page, MIN(duration_sec) AS min_duration_sec, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range
    FROM filtered_web_events
    GROUP BY page
)
SELECT page, min_duration_sec, duration_sec_range
FROM summary
WHERE duration_sec_range > 66
ORDER BY min_duration_sec DESC
