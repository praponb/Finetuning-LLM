WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(device) AS device_lower
    FROM sessions
    WHERE started_at >= DATE '2023-06-22'
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.web_event_event_time
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.event_time AS web_event_event_time
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        device,
        browser,
        TO_DATE(started_at) AS started_at_date,
        DATE_FORMAT(started_at, 'MMM yyyy') AS started_at_label,
        COALESCE(browser, 'NONE') AS browser_clean
    FROM latest_match
    WHERE started_at > TIMESTAMP '2023-04-19 12:00:00'
      AND country = 'US'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        device,
        started_at_label,
        browser_clean,
        FIRST_VALUE(browser, TRUE) OVER (PARTITION BY browser, device ORDER BY started_at_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_browser,
        LAST_VALUE(browser) IGNORE NULLS OVER (PARTITION BY device, browser ORDER BY started_at_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser,
        ROW_NUMBER() OVER (ORDER BY started_at_date, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY YEAR(started_at_date), browser ORDER BY started_at_date DESC, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at_date, id) AS event_seq_2
    FROM base
    WHERE browser IN ('Firefox', 'Edge')
)
-- Multi-level aggregation
SELECT
    CASE WHEN event_seq >= 6 THEN 'HIGH' ELSE 'LOW' END AS event_seq_tier,
    device,
    CASE WHEN seq_no >= 8 THEN 'LARGE' ELSE 'SMALL' END AS seq_no_tier,
    MAX(event_seq) FILTER (WHERE device = 'desktop') AS desktop_max_event_seq,
    COUNT(*) FILTER (WHERE device = 'tablet') AS tablet_rows,
    COUNT(*) AS num_rows,
    SUM(CASE WHEN device = 'desktop' THEN event_seq_2 ELSE 0 END) AS desktop_event_seq_2,
    SUM(event_seq_2) FILTER (WHERE device = 'desktop') AS desktop_total_event_seq_2,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_windows
GROUP BY ROLLUP (CASE WHEN event_seq >= 6 THEN 'HIGH' ELSE 'LOW' END, device, CASE WHEN seq_no >= 8 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY distinct_ids DESC;
