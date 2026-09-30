WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM web_events
    QUALIFY ROW_NUMBER() OVER (PARTITION BY page, event_type ORDER BY duration_sec DESC, id) = 1
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        a.id,
        a.session_id,
        a.event_type,
        a.page,
        a.event_time,
        a.duration_sec,
        s.browser
    FROM ranked a
    INNER JOIN sessions s
        ON a.session_id = s.id
    WHERE s.country = 'US'
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY browser ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY browser ORDER BY event_time, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM with_dims
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY browser ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(browser, TRUE) OVER (PARTITION BY session_id, event_type ORDER BY is_new_session, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_browser,
        COUNT(island_id) OVER (PARTITION BY YEAR(event_time) ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS island_id_count_90d
    FROM sessionized
),
metrics_web_events AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(duration_sec) OVER w_recent AS next_duration_sec,
        LEAD(island_id_count_90d) OVER w_recent AS next_island_id_count_90d,
        ROW_NUMBER() OVER w_recent AS event_seq,
        AVG(duration_sec) OVER (PARTITION BY session_id ORDER BY island_id_count_90d, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_5_prior
    FROM metrics
    WINDOW w_recent AS (PARTITION BY page ORDER BY is_new_session, id)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        browser,
        is_new_session,
        island_id,
        first_known_browser,
        event_seq,
        duration_sec_avg_5_prior,
        CASE WHEN duration_sec >= 441 THEN 'High' WHEN duration_sec >= 223 THEN 'Medium' ELSE 'Low' END AS duration_sec_tier
    FROM metrics_web_events
    WHERE island_id > 4
)
SELECT
    s.id,
    s.session_id,
    s.event_type,
    s.page,
    s.browser,
    s.is_new_session,
    s.duration_sec_tier,
    (SELECT AVG(t2.duration_sec) FROM web_events t2 WHERE t2.session_id = s.session_id) AS avg_web_event_duration_sec
FROM filtered s
ORDER BY is_new_session DESC;
