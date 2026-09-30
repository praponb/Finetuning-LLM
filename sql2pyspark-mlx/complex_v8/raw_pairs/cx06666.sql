WITH recent AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        csat
    FROM tickets
    WHERE priority <> 'P3'
),
with_prev AS (
    -- Previous closed_at per agent_id
    SELECT *, LAG(closed_at) OVER (PARTITION BY agent_id ORDER BY closed_at, id) AS prev_closed_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR closed_at > prev_closed_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY agent_id ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
enriched_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.csat,
        s.prev_closed_at,
        s.session_no,
        su.hired_on AS agent_hired_on
    FROM sessionized s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.hired_on BETWEEN DATE '2024-01-30' AND DATE '2025-03-30'
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.csat,
        s.prev_closed_at,
        s.session_no,
        s.agent_hired_on,
        t.id AS event_id,
        t.event_type,
        t.event_at
    FROM enriched_agent s
    JOIN ticket_events t ON t.ticket_id = s.id
)
SELECT
    id,
    agent_id,
    closed_at,
    csat,
    prev_closed_at,
    session_no,
    agent_hired_on,
    event_id,
    event_type,
    LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
    MONTH(agent_hired_on) AS agent_hired_on_month
FROM detail_rows
