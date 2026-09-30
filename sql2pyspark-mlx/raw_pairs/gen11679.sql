WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(device, '/', browser) AS device_browser,
        COALESCE(browser, 'N/A') AS browser_filled
    FROM sessions
    WHERE customer_id IS NOT NULL
      AND started_at >= TIMESTAMP '2023-06-04 04:00:00'
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.device,
        s.browser,
        s.started_at,
        s.browser_filled,
        d.age,
        d.name
    FROM valid s
    LEFT JOIN customers d
        ON s.customer_id = d.id
    WHERE d.email LIKE '%@gmail.com'
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.event_type
    FROM enriched s
    JOIN LATERAL (
            SELECT c.event_type
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        started_at,
        name,
        event_type,
        LAST_VALUE(event_type, TRUE) OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_type,
        AVG(age) OVER (PARTITION BY TO_DATE(started_at), customer_id ORDER BY started_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS age_avg_6_prior
    FROM with_last
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.event_type,
        s.last_known_event_type,
        s.age_avg_6_prior,
        latest.web_event_duration_sec,
        latest.page
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.duration_sec AS web_event_duration_sec, c.page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY age_avg_6_prior, id) AS seq_no,
        CUME_DIST() OVER (PARTITION BY event_type, page ORDER BY web_event_duration_sec DESC NULLS FIRST) AS web_event_duration_sec_cume_dist,
        SUM(web_event_duration_sec) OVER (ORDER BY web_event_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_web_event_duration_sec
    FROM with_latest
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, cumulative_web_event_duration_sec * web_event_duration_sec AS cumulative_web_event_duration_sec_times_web_event_duration_sec
    FROM scored
    WHERE page IS NOT NULL
      AND page IS NOT DISTINCT FROM '/checkout'
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        page,
        event_type,
        cumulative_web_event_duration_sec
    FROM cleaned
) src
PIVOT (
    MAX(cumulative_web_event_duration_sec) AS amount,
    COUNT(1) AS cnt
    FOR event_type IN ('search', 'click')
);
