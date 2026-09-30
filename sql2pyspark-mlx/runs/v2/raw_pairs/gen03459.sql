SELECT
    id,
    session_id,
    event_time,
    page,
    RANK() OVER (PARTITION BY session_id ORDER BY event_time DESC) AS rnk,
    ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time, id) AS row_num
FROM web_events
