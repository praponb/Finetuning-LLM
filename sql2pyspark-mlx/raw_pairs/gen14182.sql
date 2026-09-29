SELECT id, started_at, CONCAT_WS(',', device, browser) AS device_browser_key FROM sessions WHERE browser IN ('Chrome', 'Edge') AND country IS NULL
