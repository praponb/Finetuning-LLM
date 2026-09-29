WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(event_type, 1, 4) AS event_type_prefix,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        COALESCE(page, 'UNKNOWN') AS resolved_page
    FROM web_events
    WHERE event_time >= TO_TIMESTAMP('2023-07-06 12:00:00')
      AND page IS NULL
),
joined AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        d.browser,
        d.device
    FROM base src
    LEFT JOIN sessions d
        ON src.session_id = d.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT event_type, session_id
    FROM joined
    EXCEPT
    SELECT event_type, session_id
    FROM web_events
    WHERE page IN ('/product', '/cart')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN kept_keys k ON s.event_type = k.event_type AND s.session_id = k.session_id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, UPPER(device) AS device_upper
    FROM verified
    WHERE event_time >= TIMESTAMP '2023-06-19 11:00:00'
      AND browser IS NOT NULL
),
joined_web_events AS (
    -- Attach sessions attributes
    SELECT
        a.id,
        a.session_id,
        a.page,
        a.event_type_prefix,
        a.resolved_page,
        a.browser,
        a.device_upper,
        s.device AS session_device
    FROM cleaned a
    LEFT JOIN sessions s
        ON a.session_id = s.id
    WHERE s.browser IN ('Chrome', 'Edge')
),
cleaned_web_events AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        session_id,
        page,
        event_type_prefix,
        resolved_page,
        browser,
        device_upper,
        session_device,
        UPPER(session_device) AS session_device_upper,
        CONCAT_WS(',', session_device, page) AS session_device_page_key,
        COALESCE(page, 'DIRECT') AS page_clean
    FROM joined_web_events
    WHERE page IS NOT NULL
)
SELECT
    page,
    event_type_prefix,
    browser,
    device_upper,
    session_device_upper,
    session_device_page_key,
    page_clean
FROM cleaned_web_events
ORDER BY page
