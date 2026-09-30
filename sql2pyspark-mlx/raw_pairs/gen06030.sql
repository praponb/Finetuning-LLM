WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        HOUR(event_time) event_time_hour
    FROM web_events
    WHERE event_type NOT IN ('click', 'purchase', 'add_to_cart')
      AND event_time > TO_TIMESTAMP('2023-04-05 02:00:00')
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time DESC, id) AS rn
        FROM base
    ) x
    WHERE rn = 1
),
deduped_web_events AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY rn, id) AS row_num
        FROM deduped
    ) ranked
    WHERE row_num = 1
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM deduped_web_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        event_time_hour,
        is_new_session,
        island_id,
        MIN(island_id) OVER (PARTITION BY event_type ORDER BY is_new_session, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_min_island_id,
        SUM(island_id) OVER (PARTITION BY event_type ORDER BY is_new_session, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_sum_island_id,
        COUNT(island_id) OVER (PARTITION BY event_type ORDER BY is_new_session, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_count_island_id,
        MAX(island_id) OVER (PARTITION BY event_type ORDER BY is_new_session, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_max_island_id,
        FIRST_VALUE(event_time_hour, TRUE) OVER (PARTITION BY session_id ORDER BY island_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_event_time_hour
    FROM islands
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session_2
    FROM metrics
),
islands_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session_2) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id_2
    FROM with_gaps
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        src.id,
        src.session_id,
        src.event_time_hour,
        src.is_new_session,
        src.island_id,
        src.last4_sum_island_id,
        src.last4_max_island_id,
        src.is_new_session_2,
        p.device,
        p.browser,
        p.country
    FROM islands_web_events src
    LEFT JOIN sessions p
        ON src.session_id = p.id
    WHERE p.started_at >= '2024-08-01'
),
stats AS (
    -- Multi-level aggregation
    SELECT
        country,
        ROUND(SUM(is_new_session_2) / NULLIF(COUNT(CASE WHEN browser = 'Firefox' THEN 1 END), 0), 2) AS is_new_session_2_per_firefox,
        PERCENTILE_APPROX(last4_max_island_id, ARRAY(0.25, 0.5, 0.75)) AS last4_max_island_id_quartiles,
        SUM(CASE WHEN country = 'TH' THEN island_id ELSE 0 END) AS th_island_id
    FROM with_dims
    GROUP BY CUBE (country)
)
SELECT
    country,
    is_new_session_2_per_firefox,
    last4_max_island_id_quartiles,
    CASE WHEN country RLIKE '^(UK|TH)$' THEN 'OTHER_ZONE' WHEN country RLIKE '^(US|SG)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS country_zone
FROM stats
ORDER BY is_new_session_2_per_firefox DESC, country
