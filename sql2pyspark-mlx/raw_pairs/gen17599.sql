SELECT
    c.id,
    c.city,
    t.firefox_count,
    t.unique_devices
FROM customers c
LEFT JOIN (SELECT customer_id, COUNT(CASE WHEN browser = 'Firefox' THEN 1 END) AS firefox_count, COUNT(DISTINCT device) AS unique_devices
    FROM sessions
    WHERE country IS NOT NULL
    GROUP BY customer_id) t ON c.id = t.customer_id
WHERE t.firefox_count > 7
