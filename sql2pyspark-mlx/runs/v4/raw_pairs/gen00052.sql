SELECT a.*, b.country
FROM web_events a
LEFT JOIN sessions b
    ON a.session_id = b.id;
