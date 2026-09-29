SELECT device, QUARTER(started_at) AS started_at_quarter, CONCAT_WS('-', device, country) AS device_country_key
FROM sessions;
