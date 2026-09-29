SELECT started_at, TO_DATE(started_at) AS started_at_date, LOWER(device) AS device_lower
FROM sessions;
