WITH valid AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        CAST(started_at AS DATE) started_at_day
    FROM sessions
    WHERE started_at >= TO_DATE('2023-03-07')
      AND browser NOT IN ('Edge', 'Safari', 'Firefox')
)
SELECT
    DATE_TRUNC('QUARTER', started_at_day) AS quarter_start,
    COUNT_IF(device = 'mobile') AS mobile_events,
    SORT_ARRAY(COLLECT_SET(device)) AS device_list
FROM valid
GROUP BY 1
