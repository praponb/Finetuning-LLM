WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        SUBSTRING(event_type, 1, 2) AS event_type_prefix,
        COALESCE(page, 'OTHER') AS resolved_page
    FROM web_events
    WHERE event_time > TO_TIMESTAMP('2023-06-15 18:00:00')
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM filtered
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(session_start_flag) IGNORE NULLS OVER (ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_session_start_flag,
        RANK() OVER (ORDER BY session_start_flag DESC NULLS FIRST) AS session_start_flag_rank,
        STDDEV_SAMP(burst_id) OVER (PARTITION BY session_id) AS session_id_stddev_samp_burst_id
    FROM sessionized
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        s.*,
        p2.browser,
        p2.country,
        p2.device
    FROM with_windows s
    LEFT JOIN sessions p2
        ON s.session_id = p2.id
)
SELECT
    s.id,
    s.session_id,
    s.event_type,
    s.event_time,
    s.resolved_page,
    s.session_start_flag,
    s.burst_id,
    s.session_start_flag_rank,
    s.session_id_stddev_samp_burst_id,
    CASE WHEN event_type RLIKE '^(click|page_view)$' THEN 'CORE' ELSE 'GROUP_B' END AS event_type_zone,
    (SELECT MIN(x.duration_sec) FROM web_events x WHERE x.session_id = s.session_id) AS min_web_event_duration_sec
FROM with_dims s;
