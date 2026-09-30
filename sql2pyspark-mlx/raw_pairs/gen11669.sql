WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM web_events
),
islands AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
valid AS (
    SELECT
        *,
        COALESCE(page, 'unassigned') page_filled,
        LEAST(duration_sec, is_new_burst) least_duration_sec_is_new_burst,
        COALESCE(page, 'NONE') page_filled_2
    FROM islands
    WHERE session_id = 2
),
enriched AS (
    SELECT
        src.id,
        src.session_id,
        src.page,
        src.event_time,
        src.island_id,
        d.browser,
        d.country
    FROM valid src
    LEFT JOIN sessions d
        ON src.session_id = d.id
    WHERE d.country IS NOT NULL
),
with_dims AS (
    SELECT
        s.id,
        s.session_id,
        s.event_time,
        s.browser,
        s.country,
        p2.device,
        p2.country AS session_country,
        p2.browser AS session_browser
    FROM enriched s
    LEFT JOIN sessions p2
        ON s.session_id = p2.id
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time NULLS LAST, id) AS rn
        FROM with_dims
    ) t
    WHERE rn = 1
),
flagged AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_browser ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_browser ORDER BY event_time, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM deduped
),
islands_web_events AS (
    SELECT
        id,
        session_id,
        event_time,
        browser,
        session_country,
        session_browser,
        SUM(session_start_flag) OVER (PARTITION BY session_browser ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
SELECT
    s.id,
    s.session_id,
    s.event_time,
    s.browser,
    s.session_country,
    CASE WHEN browser RLIKE '^(Firefox|Chrome)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS browser_zone,
    (SELECT AVG(w2.duration_sec) FROM web_events w2 WHERE w2.session_id = s.session_id) AS avg_web_event_duration_sec
FROM islands_web_events s
ORDER BY browser
