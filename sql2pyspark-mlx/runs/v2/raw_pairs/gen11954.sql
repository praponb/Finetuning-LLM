SELECT
    id,
    customer_id,
    started_at,
    CONCAT_WS('/', device, browser) AS device_browser_key
FROM sessions
WHERE browser = 'Edge' AND customer_id = 22 AND country IN ('TH', 'US')
ORDER BY device_browser_key, id
LIMIT 20
