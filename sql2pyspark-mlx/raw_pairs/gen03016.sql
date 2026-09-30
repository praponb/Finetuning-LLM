SELECT
    id,
    browser,
    CASE WHEN browser = 'Safari' THEN 1 ELSE 0 END AS is_safari,
    LOWER(device) AS device_lower
FROM sessions
WHERE browser NOT IN ('Safari', 'Firefox', 'Edge')
ORDER BY is_safari DESC, browser, id
LIMIT 3
