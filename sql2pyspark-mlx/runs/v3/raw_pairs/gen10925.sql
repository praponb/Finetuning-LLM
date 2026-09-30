SELECT
    id,
    duration_sec,
    event_time,
    LAST_VALUE(event_time) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_event_time,
    LEAD(duration_sec, 1, 0) OVER (ORDER BY event_time, id) next_duration_sec
FROM web_events
WHERE duration_sec != 214
ORDER BY duration_sec DESC;
