WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, CAST(duration_sec AS STRING) AS duration_sec_num
    FROM web_events
    WHERE event_time > TO_TIMESTAMP('2023-03-26 07:00:00')
      AND page IS NOT NULL
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        s.id,
        s.session_id,
        s.event_type,
        s.page,
        s.duration_sec_num,
        d.browser,
        d.country,
        d.device
    FROM valid s
    LEFT JOIN sessions d
        ON s.session_id = d.id
    WHERE d.customer_id IS NULL
),
joined AS (
    -- Attach sessions attributes
    SELECT
        s.id,
        s.session_id,
        s.event_type,
        s.page,
        s.duration_sec_num,
        s.country,
        b.country AS session_country,
        b.browser AS session_browser
    FROM enriched s
    LEFT JOIN sessions b
        ON s.session_id = b.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(event_type, ' ', page) AS event_type_page,
        UPPER(page) AS page_upper
    FROM joined
    WHERE session_country IS NOT NULL
      AND country NOT IN ('TH', 'JP', 'SG')
),
filtered_web_events AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(event_type) AS clean_event_type,
        COALESCE(page, 'UNKNOWN') AS resolved_page
    FROM filtered
    WHERE session_browser IS NOT NULL
)
SELECT
    id,
    session_id,
    page,
    duration_sec_num,
    session_country,
    session_browser,
    page_upper,
    clean_event_type,
    resolved_page
FROM filtered_web_events
ORDER BY id
