WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        CASE WHEN duration_sec >= 387 THEN 'High' WHEN duration_sec >= 144 THEN 'Medium' ELSE 'Low' END AS duration_sec_tier,
        COALESCE(page, 'DIRECT') AS page_filled
    FROM web_events
    WHERE page IN ('/home', '/product', '/cart')
      AND event_time >= TO_TIMESTAMP('2023-08-01 16:00:00')
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT page, event_type
    FROM valid
    INTERSECT
    SELECT page, event_type
    FROM web_events
    WHERE event_type IN ('add_to_cart', 'click')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN remaining_keys k ON s.page = k.page AND s.event_type = k.event_type
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, page
    FROM reconciled
    INTERSECT ALL
    SELECT id, page
    FROM web_events
    WHERE TO_DATE(event_time) < '2025-10-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM reconciled s
    JOIN kept_keys k ON s.id = k.id AND s.page = k.page
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        d.browser,
        d.country
    FROM verified src
    LEFT JOIN sessions d
        ON src.session_id = d.id
    WHERE d.device = 'mobile'
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN duration_sec >= 537 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_tier,
        AVG(duration_sec) FILTER (WHERE country = 'SG') AS sg_avg_duration_sec,
        SIZE(COLLECT_SET(event_type)) AS num_event_types,
        COUNT(DISTINCT page) AS distinct_pages
    FROM with_dims
    GROUP BY duration_sec_tier
)
SELECT
    duration_sec_tier,
    sg_avg_duration_sec,
    num_event_types,
    distinct_pages
FROM stats
ORDER BY num_event_types DESC, duration_sec_tier DESC;
