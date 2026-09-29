WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(event_type) event_type_length
    FROM web_events
    WHERE page IS NOT NULL
      AND event_time >= TO_TIMESTAMP('2023-03-13 06:00:00')
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(session_no) OVER (PARTITION BY page, event_type ORDER BY session_no, id) prev_session_no,
        LAG(duration_sec) OVER (PARTITION BY page, event_type ORDER BY session_no, id) prev_duration_sec,
        LEAD(duration_sec) OVER (PARTITION BY page, event_type ORDER BY session_no, id) next_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY page, event_type ORDER BY session_no, id) seq_no
    FROM sessions_web_events
),
joined AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        p.device,
        p.browser,
        p.country
    FROM with_windows src
    LEFT JOIN sessions p
        ON src.session_id = p.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT session_id, id
    FROM joined
    EXCEPT DISTINCT
    SELECT session_id, id
    FROM web_events
    WHERE page NOT IN ('/checkout', '/search')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN matched_keys k ON s.session_id = k.session_id AND s.id = k.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        event_type,
        page,
        duration_sec,
        session_no,
        prev_session_no,
        country,
        RANK() OVER w_part next_duration_sec_rank
    FROM verified
    WINDOW w_part AS (PARTITION BY browser, page ORDER BY next_duration_sec NULLS FIRST)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        event_type,
        page,
        duration_sec,
        session_no,
        prev_session_no,
        country,
        INITCAP(TRIM(page)) page_title,
        COALESCE(page, 'N/A') page_filled
    FROM scored
    WHERE page IS NOT NULL
)
-- Grouped metrics
SELECT
    prev_session_no,
    COUNT(DISTINCT event_type) AS distinct_event_types,
    SIZE(COLLECT_SET(country)) AS num_countries,
    SUM(CASE WHEN country = 'DE' THEN session_no ELSE 0 END) AS de_session_no,
    MAX(prev_session_no) FILTER (WHERE event_type = 'page_view') AS page_view_max_prev_session_no
FROM valid
GROUP BY ROLLUP (prev_session_no)
HAVING distinct_event_types > 1
ORDER BY prev_session_no NULLS LAST
