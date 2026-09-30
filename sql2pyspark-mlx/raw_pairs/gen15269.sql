WITH base AS (
    SELECT
        *,
        CASE WHEN duration_sec >= 511 THEN 'High' WHEN duration_sec >= 155 THEN 'Medium' ELSE 'Low' END duration_sec_band,
        COALESCE(page, 'OTHER') page_clean
    FROM web_events
    WHERE page IS NOT NULL
),
with_windows AS (
    SELECT
        *,
        AVG(duration_sec) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) duration_sec_avg_5,
        FIRST_VALUE(page) IGNORE NULLS OVER (PARTITION BY page ORDER BY duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_page
    FROM base
),
with_dims AS (
    SELECT
        s.*,
        p2.country,
        p2.device,
        p2.browser
    FROM with_windows s
    LEFT JOIN sessions p2
        ON s.session_id = p2.id
),
enriched AS (
    SELECT
        src.id,
        src.session_id,
        src.event_type,
        src.event_time,
        src.page_clean,
        src.duration_sec_avg_5,
        src.country,
        p.device AS session_device,
        p.country AS session_country
    FROM with_dims src
    JOIN sessions p
        ON src.session_id = p.id
)
SELECT
    s.id,
    s.session_id,
    s.event_type,
    s.page_clean,
    s.country,
    s.session_device,
    (SELECT SUM(x.duration_sec) FROM web_events x WHERE x.session_id = s.session_id) AS sum_web_event_duration_sec
FROM enriched s
ORDER BY id DESC, session_id ASC
