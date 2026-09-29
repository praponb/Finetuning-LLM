WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device,
        browser,
        country,
        LOWER(browser) AS browser_lower,
        SUBSTRING(browser, 1, 3) AS browser_prefix,
        COALESCE(country, 'DIRECT') AS country_filled
    FROM sessions
    WHERE started_at >= '2024-03-01'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.page IN ('/search', '/product')
        )
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.web_event_page,
        latest.duration_sec
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.page AS web_event_page, c.duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.page != '/search'
        )
),
summary AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN duration_sec >= 375 THEN 'HIGH' ELSE 'LOW' END AS duration_sec_tier,
        country,
        MAX(duration_sec) FILTER (WHERE device = 'mobile') AS mobile_max_duration_sec,
        SIZE(COLLECT_SET(web_event_page)) AS num_web_event_pages,
        SUM(CASE WHEN web_event_page = '/product' THEN duration_sec ELSE 0 END) AS product_duration_sec,
        COUNT(DISTINCT web_event_page) AS distinct_web_event_pages,
        MIN(duration_sec) AS min_duration_sec,
        COUNT(*) FILTER (WHERE web_event_page = '/checkout') AS checkout_rows
    FROM kept
    GROUP BY ROLLUP (CASE WHEN duration_sec >= 375 THEN 'HIGH' ELSE 'LOW' END, country)
    HAVING MAX(duration_sec) FILTER (WHERE device = 'mobile') > 0
)
SELECT
    country,
    mobile_max_duration_sec,
    num_web_event_pages,
    product_duration_sec,
    min_duration_sec
FROM summary
ORDER BY country DESC NULLS LAST;
