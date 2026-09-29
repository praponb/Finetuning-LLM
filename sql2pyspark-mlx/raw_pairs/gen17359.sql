WITH with_last AS (
    SELECT
        s.*,
        x.event_type,
        x.web_event_page
    FROM sessions s
    LEFT JOIN LATERAL (
            SELECT c.event_type, c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    SELECT
        id,
        device,
        started_at,
        country,
        event_type,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC) AS event_seq,
        FIRST_VALUE(event_type, TRUE) OVER (PARTITION BY event_type ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_type,
        ROW_NUMBER() OVER (ORDER BY started_at DESC, id ASC) AS seq_no,
        FIRST_VALUE(country) IGNORE NULLS OVER (PARTITION BY web_event_page ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country,
        FIRST_VALUE(browser, TRUE) OVER (PARTITION BY country ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_browser
    FROM with_last
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY device ORDER BY started_at ASC, id ASC) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY device ORDER BY started_at ASC, id ASC) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM metrics
),
islands AS (
    SELECT
        id,
        device,
        started_at,
        country,
        first_known_country,
        SUM(session_start_flag) OVER (PARTITION BY device ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY session_id DESC, id ASC) AS rank_in_group
    FROM islands
    QUALIFY rank_in_group = 1
)
SELECT
    id,
    device,
    country,
    session_id
FROM deduped
ORDER BY id DESC
