SELECT *, SUBSTRING(device, 1, 2) AS device_prefix, INITCAP(TRIM(country)) AS country_title FROM sessions WHERE (country IS NULL AND country IN ('JP', 'TH')) OR customer_id IS NOT NULL;
