WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        event_type,
        duration_sec,
        CASE WHEN duration_sec >= 402 THEN 'High' WHEN duration_sec >= 174 THEN 'Medium' ELSE 'Low' END duration_sec_band,
        COALESCE(page, 'DIRECT') page_clean
    FROM web_events
    WHERE event_time >= DATE '2023-04-04'
      AND page IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        session_id,
        duration_sec,
        LEAD(duration_sec) OVER w_recent next_duration_sec,
        ROW_NUMBER() OVER w_recent seq_no
    FROM filtered
    WHERE session_id = 7
    WINDOW w_recent AS (PARTITION BY event_type, session_id ORDER BY duration_sec, id)
),
joined AS (
    -- Attach sessions attributes
    SELECT
        e.id,
        e.session_id,
        e.next_duration_sec,
        e.seq_no,
        p.device
    FROM enriched e
    INNER JOIN sessions p
        ON e.session_id = p.id
),
enriched_web_events AS (
    -- Attach sessions attributes
    SELECT
        e.*,
        p.country,
        p.browser
    FROM joined e
    LEFT JOIN sessions p
        ON e.session_id = p.id
)
-- Join a dimension and aggregate
SELECT
    dim.browser AS session_browser,
    dim.country AS session_country,
    COUNT(*) AS rows_total,
    AVG(a.seq_no) AS avg_seq_no,
    ROUND(SUM(a.next_duration_sec), 2) AS total_next_duration_sec
FROM enriched_web_events a
INNER JOIN sessions dim ON a.session_id = dim.id
GROUP BY GROUPING SETS ((dim.browser, dim.country), (dim.browser))
ORDER BY rows_total
