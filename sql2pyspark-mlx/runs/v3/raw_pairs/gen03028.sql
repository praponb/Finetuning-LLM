WITH RecentWebEvents AS (
    SELECT *
    FROM web_events
    WHERE event_type IN ('purchase', 'search', 'click')
),
agg AS (
    SELECT page, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec
    FROM RecentWebEvents
    GROUP BY page
)
SELECT *
FROM agg
WHERE stddev_duration_sec > 36
