WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT_WS('-', page, event_type) AS page_event_type_key
    FROM web_events
    WHERE page IN ('/checkout', '/cart', '/product')
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time DESC NULLS LAST, id) AS rank_in_group
        FROM filtered
    ) t
    WHERE rank_in_group = 1
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM deduped
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
deduped_web_events AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY session_id ORDER BY event_time DESC NULLS FIRST) AS rank_in_group_2
    FROM sessionized
    QUALIFY rank_in_group_2 = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY event_type, page ORDER BY session_start_flag) AS rn
        FROM deduped_web_events
    ) ranked
    WHERE rn = 1
)
SELECT
    s.id,
    s.session_id,
    s.event_type,
    s.page,
    s.page_event_type_key,
    s.rank_in_group,
    s.session_start_flag,
    (SELECT MIN(x.duration_sec) FROM web_events x WHERE x.session_id = s.session_id) AS min_web_event_duration_sec
FROM latest s
ORDER BY event_type DESC
