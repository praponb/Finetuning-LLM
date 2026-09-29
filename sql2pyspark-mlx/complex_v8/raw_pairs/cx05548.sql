WITH filtered_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type NOT IN ('assigned', 'resolved')
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM filtered_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        DAYOFWEEK(event_at) AS event_at_dayofweek,
        event_at + INTERVAL 30 MINUTES AS event_at_plus_30m
    FROM sessionized
),
combined_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.new_session_flag,
        s.session_id,
        s.event_at_dayofweek,
        s.event_at_plus_30m,
        t.channel AS ticket_channel,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM tagged s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
    WHERE t.channel <> 'chat'
)
SELECT
    id,
    actor,
    new_session_flag,
    agent_tier,
    agent_team,
    LENGTH(TRIM(actor)) AS actor_len,
    ROUND(event_at_dayofweek / NULLIF(new_session_flag, 0), 3) AS event_at_dayofweek_per_new_session_flag
FROM combined_details
WHERE new_session_flag BETWEEN 0 AND 1
ORDER BY agent_team DESC, actor_len DESC
