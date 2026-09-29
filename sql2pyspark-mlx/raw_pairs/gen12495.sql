SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id, page ORDER BY event_time, id) AS rank_in_group
    FROM web_events
) ranked
WHERE rank_in_group = 1
