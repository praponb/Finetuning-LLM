WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS(',', event_type, page) AS event_type_page_key,
        DATE_FORMAT(event_time, 'yyyy-MM-dd') AS event_time_label,
        COALESCE(page, 'DIRECT') AS resolved_page
    FROM web_events
    WHERE event_type = 'add_to_cart'
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        s.*,
        b.device,
        b.country
    FROM base s
    LEFT JOIN sessions b
        ON s.session_id = b.id
    WHERE HOUR(b.started_at) BETWEEN 12 AND 18
),
enriched_web_events AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        d.country AS session_country,
        d.device AS session_device
    FROM enriched src
    JOIN sessions d
        ON src.session_id = d.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_type_page_key,
        device,
        country,
        session_device
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC NULLS FIRST, id) AS rn
        FROM enriched_web_events
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    event_type,
    event_type_page_key,
    device,
    session_device
FROM ranked
ORDER BY session_device, id DESC;
