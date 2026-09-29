SELECT
    c.id,
    c.country,
    COUNT(s.id) AS session_count,
    COUNT(DISTINCT s.browser) AS unique_browsers,
    SORT_ARRAY(COLLECT_SET(s.device)) AS device_list
FROM customers c
LEFT OUTER JOIN sessions s ON s.customer_id = c.id
WHERE MONTH(c.signup_date) IN (2, 7, 10)
GROUP BY c.id, c.country
ORDER BY c.id, country
LIMIT 20;
