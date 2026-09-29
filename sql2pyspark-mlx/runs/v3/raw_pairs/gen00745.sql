SELECT *, SUBSTRING(page, 1, 2) page_prefix, LENGTH(page) page_length
FROM web_events
WHERE session_id IN (1, 23, 24);
