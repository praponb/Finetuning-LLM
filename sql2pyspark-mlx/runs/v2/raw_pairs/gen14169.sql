SELECT
    we.id,
    we.session_id,
    we.page,
    s.browser
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
ORDER BY s.browser, id
LIMIT 1;
