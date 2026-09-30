WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN duration_sec >= 533 THEN 'High' WHEN duration_sec >= 170 THEN 'Medium' ELSE 'Low' END AS duration_sec_band
    FROM web_events
    WHERE event_time >= TO_DATE('2023-05-02')
      AND page IS NOT NULL
)
SELECT event_type, id
FROM base
EXCEPT
SELECT event_type, id
FROM web_events
WHERE session_id IN (13, 14, 17, 40);
