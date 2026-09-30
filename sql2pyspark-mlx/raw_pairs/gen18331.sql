WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(page, '/', event_type) AS page_event_type
    FROM web_events
    WHERE page IS NOT NULL
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        a.id,
        a.session_id,
        a.event_time,
        s.country,
        s.device,
        s.browser
    FROM filtered a
    LEFT JOIN sessions s
        ON a.session_id = s.id
    WHERE s.browser IN ('Edge', 'Chrome')
),
enriched_web_events AS (
    -- Running and moving aggregates
    SELECT *, LAST_VALUE(country) IGNORE NULLS OVER w_ordered AS last_known_country
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY country ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN country = 'UK' THEN 1 ELSE 0 END AS is_uk,
        DATE_FORMAT(event_time, 'yyyy-MM-dd') AS event_time_label,
        COALESCE(browser, 'NONE') AS browser_clean
    FROM enriched_web_events
    WHERE event_time > DATE '2023-10-06'
),
filtered_web_events AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        event_time,
        country,
        device,
        browser,
        last_known_country,
        is_uk,
        browser_clean,
        ROUND(is_uk * 0.9, 2) AS is_uk_scaled,
        COALESCE(device, 'DIRECT') AS resolved_device
    FROM valid
    WHERE event_time >= DATE '2023-09-01'
      AND country IS NOT NULL
)
-- Multi-level aggregation
SELECT
    device,
    SUM(CASE WHEN browser = 'Edge' THEN is_uk_scaled ELSE 0 END) AS edge_is_uk_scaled,
    APPROX_PERCENTILE(is_uk_scaled, ARRAY(0.25, 0.5, 0.75)) AS is_uk_scaled_quartiles,
    ROUND(SUM(is_uk_scaled) / NULLIF(COUNT(CASE WHEN device = 'mobile' THEN 1 END), 0), 2) AS is_uk_scaled_per_mobile,
    COUNT_IF(country = 'US') AS us_events,
    AVG(is_uk) FILTER (WHERE device = 'mobile') AS mobile_avg_is_uk,
    SUM(is_uk) AS total_is_uk
FROM filtered_web_events
GROUP BY device
HAVING mobile_avg_is_uk < 1000000
ORDER BY mobile_avg_is_uk DESC NULLS LAST
