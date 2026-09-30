SELECT s.started_at, we.session_id, we.duration_sec, COALESCE(we.duration_sec, 0) AS duration_sec_filled FROM sessions s LEFT JOIN web_events we ON we.session_id = s.id WHERE we.duration_sec IS NULL
