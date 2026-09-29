SELECT duration_sec_bucket, event_type_values, unique_event_types
FROM (
    SELECT CASE WHEN duration_sec >= 375 THEN 'high' WHEN duration_sec >= 172 THEN 'mid' ELSE 'low' END AS duration_sec_bucket, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(event_type))) AS event_type_values, COUNT(DISTINCT duration_sec) AS unique_event_types
    FROM web_events
    GROUP BY CASE WHEN duration_sec >= 375 THEN 'high' WHEN duration_sec >= 172 THEN 'mid' ELSE 'low' END
) AS agg
WHERE unique_event_types >= 9;
