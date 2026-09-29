WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time DESC, id) AS rn FROM web_events) SELECT id, session_id, event_time FROM numbered WHERE rn <= 5 ORDER BY id
