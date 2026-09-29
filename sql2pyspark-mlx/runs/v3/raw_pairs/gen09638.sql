SELECT started_at, INITCAP(TRIM(device)) AS device_title FROM sessions WHERE TO_DATE(started_at) >= '2024-01-01' ORDER BY started_at DESC;
