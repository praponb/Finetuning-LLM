WITH metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        device,
        browser,
        country,
        ROW_NUMBER() OVER (PARTITION BY TO_DATE(started_at), device ORDER BY started_at, id) AS event_seq,
        LAST_VALUE(browser) IGNORE NULLS OVER w_hist AS last_known_browser,
        LAST_VALUE(browser) IGNORE NULLS OVER w_hist AS last_known_browser_2,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', started_at), customer_id ORDER BY started_at DESC, id) AS seq_no
    FROM sessions
    WHERE country = 'JP'
    WINDOW w_hist AS (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.web_event_duration_sec
    FROM metrics s
    JOIN LATERAL (
            SELECT c.duration_sec AS web_event_duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY web_event_duration_sec, id) AS dedup_rank
        FROM with_latest
    ) x
    WHERE dedup_rank = 1
),
metrics_sessions AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(web_event_duration_sec) OVER (PARTITION BY country ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_web_event_duration_sec,
        MAX(seq_no) OVER (PARTITION BY customer_id, device) AS customer_id_max_seq_no
    FROM latest
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.web_event_event_type
    FROM metrics_sessions s
    JOIN LATERAL (
            SELECT c.event_type AS web_event_event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
matched_keys AS (
    -- Set difference on keys
    SELECT country, id
    FROM latest_match
    EXCEPT ALL
    SELECT country, id
    FROM sessions
    WHERE started_at < '2023-09-29'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest_match s
    INNER JOIN matched_keys k ON s.country = k.country AND s.id = k.id
)
-- Join a dimension and aggregate
SELECT
    dim.country AS customer_country,
    e.browser,
    COUNT(*) AS num_rows,
    MAX(e.web_event_duration_sec) AS max_web_event_duration_sec,
    SUM(e.running_web_event_duration_sec) AS total_running_web_event_duration_sec,
    GROUPING_ID() AS level_id
FROM reconciled e
LEFT JOIN customers dim ON e.customer_id = dim.id
GROUP BY ROLLUP (dim.country, e.browser);
