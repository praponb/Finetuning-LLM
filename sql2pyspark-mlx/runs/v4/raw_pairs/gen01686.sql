WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN browser IN ('Edge', 'Chrome') THEN 'Core' ELSE 'Other' END AS browser_group,
        COALESCE(country, 'DIRECT') AS country_filled
    FROM sessions
    WHERE started_at > TIMESTAMP '2023-12-30 17:00:00'
),
enriched AS (
    -- Attach customers attributes
    SELECT s.*, p.city
    FROM base s
    LEFT JOIN customers p
        ON s.customer_id = p.id
),
kept_keys AS (
    -- Set intersection on keys
    SELECT customer_id, device
    FROM enriched
    INTERSECT
    SELECT customer_id, device
    FROM sessions
    WHERE country = 'SG'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN kept_keys k ON s.customer_id = k.customer_id AND s.device = k.device
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        browser_group,
        country_filled,
        city,
        INITCAP(TRIM(country)) AS country_title,
        COALESCE(city, 'OTHER') AS city_clean
    FROM verified
    WHERE started_at > TIMESTAMP '2024-01-10 14:00:00'
      AND started_at >= '2023-09-01'
),
matched_keys AS (
    -- Set intersection on keys
    SELECT customer_id
    FROM filtered
    INTERSECT ALL
    SELECT customer_id
    FROM sessions
    WHERE TO_DATE(started_at) >= '2023-09-03'
),
verified_sessions AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN matched_keys k ON s.customer_id = k.customer_id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device,
        browser,
        country,
        browser_group,
        country_filled,
        country_title,
        city_clean,
        DATE_FORMAT(started_at, 'yyyy-MM') AS started_at_label,
        INITCAP(TRIM(city)) AS city_title,
        COALESCE(city, 'DIRECT') AS city_filled
    FROM verified_sessions
    WHERE started_at > DATE '2023-05-25'
),
base_sessions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        device,
        browser,
        country,
        browser_group,
        country_filled,
        country_title,
        started_at_label,
        city_title,
        city_filled,
        LOWER(device) AS device_lower
    FROM cleaned
    WHERE device = 'mobile'
),
daily AS (
    -- Multi-level aggregation
    SELECT
        browser,
        COUNT_IF(browser = 'Firefox') AS firefox_events,
        COUNT_IF(country = 'TH') AS th_events
    FROM base_sessions
    GROUP BY browser
)
SELECT browser, firefox_events
FROM daily
ORDER BY browser ASC
LIMIT 10;
