SELECT we.session_id, we.page, s.browser
FROM web_events we
LEFT JOIN sessions s ON s.id = we.session_id
WHERE s.country != 'US'
ORDER BY browser ASC;
