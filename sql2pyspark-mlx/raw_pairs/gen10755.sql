WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        page,
        duration_sec,
        CASE WHEN page IN ('/search', '/cart') THEN 'Priority' ELSE 'Other' END AS page_group
    FROM web_events
    WHERE event_time > DATE '2023-05-07'
      AND page IS NOT NULL
),
enriched AS (
    -- Attach sessions attributes
    SELECT src.*, p.country
    FROM filtered src
    LEFT JOIN sessions p
        ON src.session_id = p.id
),
matched_keys AS (
    -- Set intersection on keys
    SELECT event_type, id
    FROM enriched
    INTERSECT
    SELECT event_type, id
    FROM web_events
    WHERE session_id IN (16, 24, 31)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.event_type = k.event_type AND s.id = k.id
),
joined AS (
    -- Attach sessions attributes
    SELECT
        e.id,
        e.session_id,
        e.event_type,
        e.page,
        e.duration_sec,
        e.country,
        p.browser,
        p.device,
        p.country AS session_country
    FROM verified e
    LEFT JOIN sessions p
        ON e.session_id = p.id
)
-- Join a dimension and aggregate
SELECT
    p.browser AS session_browser,
    a.page,
    COUNT(*) AS cnt,
    ROUND(SUM(a.duration_sec), 2) AS total_duration_sec
FROM joined a
LEFT JOIN sessions p ON a.session_id = p.id
GROUP BY p.browser, a.page;
