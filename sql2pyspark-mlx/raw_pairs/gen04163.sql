WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(page) clean_page,
        COALESCE(page, 'DIRECT') page_filled
    FROM web_events
    WHERE event_time >= TIMESTAMP '2023-04-02 14:00:00'
      AND page IS NOT DISTINCT FROM '/home'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY page, session_id ORDER BY duration_sec NULLS FIRST, id) = 1
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) AS prev_event_time
    FROM ranked
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM sessionized
    QUALIFY RANK() OVER (PARTITION BY page ORDER BY duration_sec DESC) = 1
)
-- Ranking and gap features
SELECT
    *,
    FIRST_VALUE(page) IGNORE NULLS OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_page,
    AVG(is_new_burst) OVER (PARTITION BY session_id, event_type ORDER BY CAST(prev_event_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) is_new_burst_avg_90d,
    STDDEV_SAMP(is_new_burst) OVER (PARTITION BY page) page_stddev_samp_is_new_burst,
    SUM(is_new_burst) OVER (PARTITION BY YEAR(event_time), event_type ORDER BY session_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_is_new_burst
FROM deduped
