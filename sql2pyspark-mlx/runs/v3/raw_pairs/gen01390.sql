SELECT duration_sec, CEIL(duration_sec) AS duration_sec_whole, DATE_TRUNC('MONTH', event_time) AS event_time_period
FROM web_events
WHERE duration_sec BETWEEN 173 AND 192 AND event_type NOT IN ('add_to_cart', 'purchase', 'search')
