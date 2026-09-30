WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        started_at,
        INITCAP(TRIM(device)) AS device_title,
        DATE_FORMAT(started_at, 'yyyy-MM-dd') AS started_at_label,
        COALESCE(browser, 'NONE') AS browser_clean
    FROM sessions
    WHERE started_at > DATE '2023-03-16'
      AND browser IN ('Safari', 'Edge')
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.started_at,
        s.device_title,
        s.started_at_label,
        s.browser_clean,
        top1.page,
        top1.web_event_event_time
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.page, c.event_time AS web_event_event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(web_event_event_time) OVER (PARTITION BY page ORDER BY web_event_event_time ASC, id ASC) IS NULL
                 OR web_event_event_time > LAG(web_event_event_time) OVER (PARTITION BY page ORDER BY web_event_event_time ASC, id ASC) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_latest
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY page ORDER BY web_event_event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        started_at,
        browser_clean,
        page,
        is_new_burst
    FROM sessions_sessions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY page ORDER BY web_event_event_time ASC NULLS LAST, id ASC) = 1
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time >= s.started_at
        )
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        started_at,
        browser_clean,
        LEAD(is_new_burst, 2) OVER w_grp AS next_is_new_burst,
        NTILE(5) OVER (ORDER BY is_new_burst DESC, id ASC) AS is_new_burst_bucket,
        SUM(is_new_burst) OVER (PARTITION BY page ORDER BY is_new_burst ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_new_burst
    FROM not_flagged
    WINDOW w_grp AS (PARTITION BY page ORDER BY started_at ASC, id ASC)
)
-- Running and moving aggregates
SELECT
    *,
    SUM(cumulative_is_new_burst) OVER (ORDER BY cumulative_is_new_burst ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_cumulative_is_new_burst,
    COUNT(cumulative_is_new_burst) OVER (ORDER BY cumulative_is_new_burst ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_cumulative_is_new_burst,
    MIN(cumulative_is_new_burst) OVER (ORDER BY cumulative_is_new_burst ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_cumulative_is_new_burst
FROM scored
