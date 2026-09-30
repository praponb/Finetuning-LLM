SELECT device, country, TO_DATE(started_at) AS started_at_date, UPPER(device) AS device_upper FROM sessions ORDER BY device DESC;
