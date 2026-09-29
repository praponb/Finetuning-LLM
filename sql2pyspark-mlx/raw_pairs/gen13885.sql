WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(page, 'unassigned') page_filled
    FROM web_events
    WHERE event_type != 'add_to_cart'
      AND event_time >= TO_TIMESTAMP('2023-08-10 08:00:00')
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page, session_id ORDER BY event_time DESC NULLS FIRST, id) AS dedup_rank
    FROM cleaned
    QUALIFY dedup_rank = 1
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        src.id,
        src.session_id,
        src.event_type,
        src.page,
        src.event_time,
        src.duration_sec,
        src.page_filled,
        src.dedup_rank,
        p.country,
        p.device,
        p.browser
    FROM latest src
    LEFT JOIN sessions p
        ON src.session_id = p.id
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        d.country AS session_country,
        d.device AS session_device
    FROM enriched src
    LEFT JOIN sessions d
        ON src.session_id = d.id
    WHERE d.country = 'JP'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(page)) page_title
    FROM with_dims
    WHERE browser IS NOT NULL
),
with_dims_web_events AS (
    -- Attach sessions attributes
    SELECT
        s.id,
        s.session_id,
        s.page,
        s.event_time,
        s.duration_sec,
        s.dedup_rank,
        s.country,
        s.session_device,
        s.page_title,
        b.browser AS session_browser,
        b.country AS session_country_2,
        b.device AS session_device_2
    FROM filtered s
    LEFT JOIN sessions b
        ON s.session_id = b.id
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        page,
        SIZE(COLLECT_SET(session_device)) AS num_session_devices,
        ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
        COUNT(*) FILTER (WHERE session_country_2 = 'US') AS us_rows
    FROM with_dims_web_events
    GROUP BY page
)
SELECT
    page,
    num_session_devices,
    avg_duration_sec,
    us_rows
FROM stats
ORDER BY page DESC
