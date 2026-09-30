WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec
    FROM web_events
    QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time DESC NULLS LAST, id) = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(page, 'unassigned') page_filled
    FROM ranked
    WHERE event_time >= TO_DATE('2023-05-20')
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM base
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        session_id,
        page,
        event_time,
        SUM(new_group_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_time,
        burst_id,
        UPPER(page) page_upper,
        COALESCE(page, 'N/A') page_clean
    FROM sessionized
    WHERE event_time < '2024-10-01'
),
cleaned_web_events AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN burst_id >= 8 THEN 'High' WHEN burst_id >= 2 THEN 'Medium' ELSE 'Low' END burst_id_level,
        CASE WHEN burst_id >= 7 THEN 'High' WHEN burst_id >= 2 THEN 'Medium' ELSE 'Low' END burst_id_tier
    FROM cleaned
    WHERE session_id IN (17, 23)
),
stats AS (
    -- Multi-level aggregation
    SELECT
        session_id,
        burst_id,
        COUNT(DISTINCT session_id) AS distinct_session_ids,
        COUNT(*) AS row_count
    FROM cleaned_web_events
    GROUP BY ROLLUP (session_id, burst_id)
    HAVING row_count < 1000
)
SELECT distinct_session_ids
FROM stats
ORDER BY distinct_session_ids NULLS LAST;
