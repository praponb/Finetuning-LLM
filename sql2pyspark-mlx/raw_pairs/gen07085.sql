SELECT
    country,
    device,
    SIZE(COLLECT_SET(device)) AS num_devices,
    SIZE(COLLECT_SET(country)) AS num_countries,
    SORT_ARRAY(COLLECT_SET(device)) AS device_list,
    PERCENT_RANK() OVER (PARTITION BY country ORDER BY SIZE(COLLECT_SET(device)) DESC) AS num_devices_percent_rank
FROM sessions
GROUP BY country, device
