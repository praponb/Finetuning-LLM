WITH remaining_keys AS (
    SELECT browser, device
    FROM sessions
    EXCEPT DISTINCT
    SELECT browser, device
    FROM sessions
    WHERE started_at >= '2023-09-01'
),
reconciled AS (
    SELECT s.*
    FROM sessions s
    JOIN remaining_keys k ON s.browser = k.browser AND s.device = k.device
)
SELECT
    DATE_TRUNC('WEEK', started_at) AS week_start,
    COUNT(CASE WHEN browser = 'Chrome' THEN 1 END) AS chrome_count,
    SIZE(COLLECT_SET(browser)) AS num_browsers,
    MIN(started_at) AS started_at,
    COUNT(DISTINCT device) AS distinct_devices,
    SIZE(COLLECT_SET(device)) AS num_devices,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list
FROM reconciled
GROUP BY 1
HAVING num_browsers > 1;
