WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), event_time) / 30 AS months_since_event_time,
        COALESCE(page, 'NONE') AS page_clean
    FROM web_events
    WHERE page IS NOT NULL
      AND duration_sec != 349
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time DESC NULLS LAST, id) AS row_num
        FROM base
    ) x
    WHERE row_num = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(months_since_event_time) OVER (PARTITION BY session_id ORDER BY row_num, id) AS next_months_since_event_time,
        LEAD(duration_sec) OVER (PARTITION BY session_id ORDER BY row_num, id) AS next_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY row_num, id) AS event_seq
    FROM latest
),
latest_web_events AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY row_num, id) = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(months_since_event_time, TRUE) OVER (PARTITION BY page ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_months_since_event_time,
        COUNT(months_since_event_time) OVER (PARTITION BY YEAR(event_time), page ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS months_since_event_time_count_30d,
        AVG(next_duration_sec) OVER (PARTITION BY TO_DATE(event_time), session_id ORDER BY event_seq, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS next_duration_sec_avg_6_prior,
        AVG(next_duration_sec) OVER (PARTITION BY DATE_TRUNC('week', event_time) ORDER BY row_num, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS next_duration_sec_avg_3
    FROM latest_web_events
)
-- Multi-level aggregation
SELECT
    event_type,
    SIZE(COLLECT_SET(event_type)) AS num_event_types,
    COUNT(*) FILTER (WHERE page = '/home') AS home_rows,
    MAX(event_time) AS last_event_time,
    ROUND(SUM(event_seq) / NULLIF(COUNT(CASE WHEN event_type = 'purchase' THEN 1 END), 0), 2) AS event_seq_per_purchase
FROM enriched
GROUP BY event_type
ORDER BY event_seq_per_purchase NULLS FIRST;
