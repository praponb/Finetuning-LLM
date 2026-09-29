SELECT id, device, started_at, country, SUBSTRING(device, 1, 4) AS device_prefix FROM sessions WHERE device IN ('tablet', 'desktop') AND country != 'US' AND country IN ('US', 'JP', 'SG');
