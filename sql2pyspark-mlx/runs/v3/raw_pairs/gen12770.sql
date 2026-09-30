SELECT
    id,
    device,
    CONCAT(country, '/', browser) AS country_browser,
    CONCAT_WS('-', device, country) AS device_country_key
FROM sessions
WHERE started_at >= '2023-11-01' AND device NOT IN ('tablet', 'mobile', 'desktop')
