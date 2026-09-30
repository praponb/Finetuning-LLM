SELECT
    id,
    session_id,
    event_time,
    page,
    LAST_VALUE(page) OVER (PARTITION BY session_id ORDER BY event_time DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_page,
    PERCENT_RANK() OVER (PARTITION BY page ORDER BY event_time DESC) AS event_time_pct_rank
FROM web_events;
