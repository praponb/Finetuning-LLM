WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM sessions s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.session_id = 22
        )
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time >= s.started_at
        )
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM eligible
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device,
        browser,
        started_at,
        country,
        SUM(is_new_burst) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(session_no) OVER (PARTITION BY MONTH(started_at), device) AS started_at_month_count_session_no,
        SUM(session_no) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_session_no,
        RANK() OVER (PARTITION BY country ORDER BY session_no) AS session_no_rank,
        NTILE(4) OVER (PARTITION BY TO_DATE(started_at) ORDER BY session_no DESC, id) AS session_no_bucket
    FROM sessions_sessions
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.device,
        s.country,
        s.started_at_month_count_session_no,
        s.session_no_rank,
        s.session_no_bucket,
        x.web_event_event_time
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.event_time AS web_event_event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
remaining_keys AS (
    -- Set difference on keys
    SELECT device, id
    FROM with_latest
    EXCEPT DISTINCT
    SELECT device, id
    FROM sessions
    WHERE HOUR(started_at) BETWEEN 10 AND 15
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_latest s
    INNER JOIN remaining_keys k ON s.device = k.device AND s.id = k.id
)
SELECT
    id,
    device,
    web_event_event_time
FROM reconciled
ORDER BY device NULLS LAST
