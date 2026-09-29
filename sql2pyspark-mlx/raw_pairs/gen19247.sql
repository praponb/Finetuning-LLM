WITH enriched AS (
    SELECT
        id,
        device,
        browser,
        started_at,
        country,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at, id) AS seq_no,
        FIRST_VALUE(browser, TRUE) OVER (PARTITION BY browser, device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_browser
    FROM sessions
),
with_latest AS (
    SELECT
        s.*,
        top1.event_time,
        top1.page
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.event_time, c.page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    SELECT
        id,
        device,
        browser,
        first_known_browser,
        page,
        LAST_VALUE(browser) IGNORE NULLS OVER (PARTITION BY browser ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser,
        AVG(seq_no) OVER (PARTITION BY country, page ORDER BY event_time, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS seq_no_avg_5,
        ROW_NUMBER() OVER (ORDER BY seq_no, id) AS seq_no_2
    FROM with_latest
),
remaining_keys AS (
    SELECT device
    FROM with_windows
    EXCEPT DISTINCT
    SELECT device
    FROM sessions
    WHERE started_at >= '2023-12-13'
),
reconciled AS (
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.device = k.device
),
enriched_sessions AS (
    SELECT
        device,
        browser,
        page,
        seq_no_avg_5,
        CUME_DIST() OVER (PARTITION BY device ORDER BY seq_no_2) AS seq_no_2_cume_dist,
        ROUND(seq_no_2 / SUM(seq_no_2) OVER (PARTITION BY page), 4) AS seq_no_2_share,
        ROUND(seq_no_avg_5 / SUM(seq_no_avg_5) OVER (PARTITION BY page, browser), 4) AS seq_no_avg_5_share
    FROM reconciled
    WHERE browser NOT IN ('Firefox', 'Chrome')
),
matched_keys AS (
    SELECT device, browser
    FROM enriched_sessions
    EXCEPT ALL
    SELECT device, browser
    FROM sessions
    WHERE browser = 'Chrome'
),
kept_rows AS (
    SELECT s.*
    FROM enriched_sessions s
    INNER JOIN matched_keys k ON s.device = k.device AND s.browser = k.browser
),
cleaned AS (
    SELECT *, CONCAT_WS('-', device, browser) AS device_browser_key
    FROM kept_rows
    WHERE device NOT IN ('tablet', 'desktop')
)
SELECT *
FROM (
    SELECT
        browser,
        page,
        device,
        seq_no_2_cume_dist
    FROM cleaned
) src
PIVOT (
    SUM(seq_no_2_cume_dist) AS value_sum
    FOR device IN ('tablet' AS tablet, 'desktop' AS desktop, 'mobile' AS mobile)
)
