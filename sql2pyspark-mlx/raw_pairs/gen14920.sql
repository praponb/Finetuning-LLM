SELECT *, TRIM(page) AS clean_page
FROM web_events
WHERE event_type NOT IN ('click', 'add_to_cart', 'purchase')
ORDER BY clean_page DESC, session_id DESC, id
LIMIT 5;
