SELECT
    id,
    page,
    duration_sec,
    session_id
FROM (
    SELECT *, RANK() OVER (PARTITION BY page ORDER BY duration_sec DESC) AS row_num
    FROM web_events
) AS r
WHERE row_num <= 2
ORDER BY duration_sec DESC;
