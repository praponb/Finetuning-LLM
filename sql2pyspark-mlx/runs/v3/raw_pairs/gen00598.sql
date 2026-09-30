SELECT *, CASE WHEN page = '/home' THEN 1 ELSE 0 END AS is_home
FROM web_events
WHERE page = '/product';
