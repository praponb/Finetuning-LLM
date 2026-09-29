WITH with_dims AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        p.device,
        p.country,
        p.browser
    FROM web_events src
    JOIN sessions p
        ON src.session_id = p.id
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time ASC, id ASC) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time ASC, id ASC) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM with_dims
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY page ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
joined AS (
    -- Attach sessions attributes
    SELECT
        src.id,
        src.session_id,
        src.event_type,
        src.page,
        src.island_id,
        b.browser AS session_browser
    FROM sessions_web_events src
    LEFT JOIN sessions b
        ON src.session_id = b.id
    WHERE b.browser = 'Safari'
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        e.id,
        e.session_id,
        e.event_type,
        e.island_id,
        d.country,
        d.browser,
        d.device
    FROM joined e
    JOIN sessions d
        ON e.session_id = d.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        event_type,
        island_id,
        country,
        device,
        AVG(island_id) OVER (PARTITION BY browser) browser_avg_island_id,
        NTILE(3) OVER (ORDER BY island_id ASC, id ASC) island_id_bucket,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY island_id ASC, id ASC) event_seq
    FROM enriched
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        island_id,
        country,
        device,
        browser_avg_island_id,
        INITCAP(TRIM(event_type)) event_type_title
    FROM metrics
    WHERE device IS DISTINCT FROM 'desktop'
)
SELECT
    id,
    island_id,
    country,
    device,
    browser_avg_island_id,
    event_type_title
FROM cleaned
ORDER BY island_id DESC
