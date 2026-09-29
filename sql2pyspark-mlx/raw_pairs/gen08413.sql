WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(duration_sec, 2) AS duration_sec_rounded,
        DATE_FORMAT(event_time, 'yyyy-MM-dd') AS event_time_label,
        COALESCE(page, 'NONE') AS resolved_page
    FROM web_events
    WHERE event_time > DATE '2023-09-19'
      AND page IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec_rounded,
        event_time_label,
        resolved_page
    FROM filtered
    QUALIFY RANK() OVER (PARTITION BY session_id ORDER BY duration_sec_rounded DESC) = 1
),
latest_web_events AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time DESC, id) = 1
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        d.country,
        d.browser,
        d.device
    FROM latest_web_events src
    LEFT JOIN sessions d
        ON src.session_id = d.id
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        a.*,
        s.country AS session_country,
        s.device AS session_device
    FROM enriched a
    INNER JOIN sessions s
        ON a.session_id = s.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        device,
        session_country,
        duration_sec_rounded
    FROM with_dims
) src
PIVOT (
    SUM(duration_sec_rounded)
    FOR session_country IN ('JP' AS jp, 'DE' AS de)
)
