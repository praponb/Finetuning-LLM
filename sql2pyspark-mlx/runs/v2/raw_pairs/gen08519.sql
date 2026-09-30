SELECT event_quarter, page_values, pct_search
FROM (
    SELECT QUARTER(event_time) AS event_quarter, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) AS page_values, ROUND((100 * SUM(CASE WHEN event_type = 'search' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_search
    FROM web_events
    WHERE event_type IN ('add_to_cart', 'page_view')
    GROUP BY QUARTER(event_time)
) AS s
WHERE pct_search > 43
ORDER BY pct_search DESC;
