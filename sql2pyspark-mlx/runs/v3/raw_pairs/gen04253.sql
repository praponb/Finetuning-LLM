WITH filtered_web_events AS (
    SELECT *, LOWER(event_type) event_type_lower
    FROM web_events
    WHERE page NOT IN ('/home', '/product', '/search') AND duration_sec BETWEEN 161 AND 344 AND event_type IN ('page_view', 'click', 'search')
),
PageSummary AS (
    SELECT page, SUM(CASE WHEN page = '/checkout' THEN 1 ELSE 0 END) checkout_count
    FROM filtered_web_events
    GROUP BY page
)
SELECT *
FROM PageSummary
WHERE checkout_count >= 79
ORDER BY page ASC
