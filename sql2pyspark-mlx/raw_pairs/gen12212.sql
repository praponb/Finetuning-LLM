WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(duration_sec, 2) duration_sec_rounded,
        COALESCE(page, 'N/A') page_filled
    FROM web_events
    WHERE event_time > TIMESTAMP '2023-10-18 11:00:00'
      AND page IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        page_filled,
        LAG(duration_sec_rounded) OVER w_hist prev_duration_sec_rounded,
        LAG(duration_sec) OVER w_hist prev_duration_sec,
        ROW_NUMBER() OVER w_hist row_seq
    FROM base
    WHERE duration_sec BETWEEN 219 AND 344
    WINDOW w_hist AS (PARTITION BY event_type ORDER BY duration_sec ASC, id ASC)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER w_running seq_no,
        ROW_NUMBER() OVER w_ordered event_seq
    FROM metrics
    WINDOW w_running AS (PARTITION BY TO_DATE(event_time) ORDER BY prev_duration_sec_rounded DESC, id ASC), w_ordered AS (PARTITION BY event_type ORDER BY event_time DESC, id ASC)
),
enriched_web_events AS (
    -- Attach sessions attributes
    SELECT
        e.id,
        e.session_id,
        e.page,
        e.event_time,
        e.page_filled,
        e.prev_duration_sec,
        e.seq_no,
        e.event_seq,
        p.browser,
        p.device
    FROM enriched e
    INNER JOIN sessions p
        ON e.session_id = p.id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT page
    FROM enriched_web_events
    EXCEPT DISTINCT
    SELECT page
    FROM web_events
    WHERE duration_sec BETWEEN 235 AND 470
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched_web_events s
    INNER JOIN remaining_keys k ON s.page = k.page
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        a.session_id,
        a.page,
        a.page_filled,
        a.prev_duration_sec,
        a.event_seq,
        a.browser,
        a.device,
        b.device AS session_device,
        b.browser AS session_browser
    FROM verified a
    INNER JOIN sessions b
        ON a.session_id = b.id
),
with_dims_web_events AS (
    -- Attach sessions attributes
    SELECT
        s.session_id,
        s.page_filled,
        s.prev_duration_sec,
        s.browser,
        s.session_device,
        p2.country,
        p2.browser AS session_browser_2
    FROM with_dims s
    LEFT JOIN sessions p2
        ON s.session_id = p2.id
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN prev_duration_sec >= 398 THEN 'HIGH' ELSE 'LOW' END AS prev_duration_sec_tier,
        ROUND(SUM(prev_duration_sec) / NULLIF(COUNT(CASE WHEN country = 'DE' THEN 1 END), 0), 2) AS prev_duration_sec_per_de,
        COUNT(CASE WHEN session_browser_2 = 'Edge' THEN 1 END) AS edge_count,
        MIN(prev_duration_sec) AS min_prev_duration_sec,
        ROUND(SUM(prev_duration_sec) / NULLIF(COUNT(CASE WHEN browser = 'Edge' THEN 1 END), 0), 2) AS prev_duration_sec_per_edge
    FROM with_dims_web_events
    GROUP BY CASE WHEN prev_duration_sec >= 398 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    prev_duration_sec_tier,
    edge_count,
    min_prev_duration_sec,
    prev_duration_sec_per_edge
FROM stats
