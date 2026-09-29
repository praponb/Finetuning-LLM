WITH base AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(device) AS device_lower
    FROM sessions
    WHERE started_at >= TIMESTAMP '2023-08-25 03:00:00'
      AND customer_id IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY browser, customer_id ORDER BY started_at DESC, id) AS seq_no,
        LAST_VALUE(country) IGNORE NULLS OVER w_recent AS last_known_country,
        FIRST_VALUE(browser) IGNORE NULLS OVER w_hist AS first_known_browser,
        FIRST_VALUE(browser, TRUE) OVER w_hist AS first_known_browser_2,
        LAST_VALUE(country) IGNORE NULLS OVER w_grp AS last_known_country_2
    FROM base
    WINDOW w_recent AS (PARTITION BY DATE_TRUNC('YEAR', started_at) ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.device,
        s.browser,
        s.started_at,
        s.country,
        s.first_known_browser,
        s.first_known_browser_2,
        s.last_known_country_2,
        x.web_event_page
    FROM scored s
    JOIN LATERAL (
            SELECT c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
matched_keys AS (
    -- Set difference on keys
    SELECT browser, device
    FROM with_last
    EXCEPT DISTINCT
    SELECT browser, device
    FROM sessions
    WHERE customer_id IN (3, 4, 8, 26)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_last s
    INNER JOIN matched_keys k ON s.browser = k.browser AND s.device = k.device
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        device,
        browser,
        country,
        first_known_browser,
        first_known_browser_2,
        last_known_country_2,
        web_event_page,
        COALESCE(country, 'Unknown') AS country_filled,
        TRIM(device) AS clean_device,
        COALESCE(browser, 'N/A') AS resolved_browser
    FROM kept_rows
    WHERE web_event_page IS NOT NULL
)
-- Grouped metrics
SELECT
    browser,
    COUNT(*) FILTER (WHERE browser = 'Edge') AS edge_rows,
    COUNT(*) AS event_count,
    SORT_ARRAY(COLLECT_SET(web_event_page)) AS web_event_page_list,
    COUNT(*) AS row_count,
    COUNT_IF(browser = 'Edge') AS edge_events,
    COUNT(CASE WHEN device = 'tablet' THEN 1 END) AS tablet_count
FROM valid
GROUP BY browser;
