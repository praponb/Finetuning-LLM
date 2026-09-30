WITH with_dims AS (
    -- Attach sessions attributes
    SELECT
        a.*,
        p.device,
        p.country,
        p.browser
    FROM web_events a
    LEFT JOIN sessions p
        ON a.session_id = p.id
    WHERE p.browser IN ('Chrome', 'Edge')
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time DESC, id) AS rn
        FROM with_dims
    ) x
    WHERE rn = 1
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(browser, 'unassigned') AS browser_filled,
        COALESCE(browser, 'DIRECT') AS resolved_browser
    FROM deduped
    WHERE event_time > TIMESTAMP '2023-07-06 07:00:00'
),
deduped_web_events AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page, browser ORDER BY duration_sec DESC NULLS FIRST, id) AS dedup_rank
        FROM filtered
    ) t
    WHERE dedup_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    session_id,
    COUNT(CASE WHEN country = 'UK' THEN 1 END) AS uk_count,
    COUNT(CASE WHEN country = 'UK' THEN 1 END) AS uk_count_2,
    SIZE(COLLECT_SET(browser)) AS num_browsers
FROM deduped_web_events
GROUP BY 1
HAVING num_browsers >= 1
