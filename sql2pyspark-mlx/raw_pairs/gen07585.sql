WITH filtered AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        UPPER(event_type) AS event_type_upper
    FROM web_events
    WHERE session_id IN (21, 29, 38, 40)
),
enriched AS (
    SELECT a.*, p.country
    FROM filtered a
    LEFT JOIN sessions p
        ON a.session_id = p.id
),
valid AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        event_type_upper,
        country,
        CAST(duration_sec AS STRING) AS duration_sec_str
    FROM enriched
    WHERE page IS NOT NULL
      AND page IS NOT NULL
),
ranked AS (
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time, id) = 1
),
valid_web_events AS (
    SELECT *, DATE_TRUNC('WEEK', event_time) AS event_time_period
    FROM ranked
    WHERE event_type IN ('search', 'add_to_cart', 'purchase')
)
SELECT
    page,
    SIZE(COLLECT_SET(page)) AS num_pages,
    APPROX_PERCENTILE(duration_sec, 0.95) AS duration_sec_p95,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes,
    MAX(event_time) AS last_seen,
    STDDEV_SAMP(duration_sec) AS duration_sec_stddev
FROM valid_web_events
GROUP BY page
ORDER BY span_minutes
