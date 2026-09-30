WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        device,
        started_at,
        country,
        CASE WHEN browser IN ('Chrome', 'Safari') THEN 'Core' ELSE 'Other' END AS browser_group
    FROM sessions
    WHERE browser IS NOT NULL
      AND country IN ('US', 'UK')
)
-- Multi-level aggregation
SELECT
    device,
    COUNT_IF(device = 'tablet') AS tablet_events,
    MIN(started_at) AS started_at
FROM cleaned
GROUP BY device
