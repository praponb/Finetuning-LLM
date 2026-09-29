WITH valid AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DAYOFWEEK(event_at) AS event_at_dayofweek
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY actor, event_type) AS rows_in_actor_event_type,
        MAX(event_at_dayofweek) OVER (ORDER BY starts_session DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS event_at_dayofweek_max_2_prior,
        ROW_NUMBER() OVER (ORDER BY starts_session DESC, id) AS seq_no
    FROM sessionized
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, metric_value
FROM (
    SELECT id, seq_no, session_no
    FROM metrics
) src
UNPIVOT (
    metric_value FOR field IN (seq_no AS seq_no_v, session_no AS m_session_no)
) unp
ORDER BY id, field
