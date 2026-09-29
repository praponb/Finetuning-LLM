WITH cleaned_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per ticket_id
    SELECT *, LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev_event_at
    FROM cleaned_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        ticket_id,
        session_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY ticket_id, session_id
),
enriched_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.ticket_id,
        s.session_id,
        s.session_start,
        s.session_end,
        s.duration_min,
        t.channel AS ticket_channel,
        su.hired_on AS agent_hired_on
    FROM per_session s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
    WHERE t.channel = 'email'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(duration_min) OVER w_agent_hired_on_month, 2) AS agent_hired_on_month_avg_duration_min,
        MAX(session_id) OVER (PARTITION BY MONTH(agent_hired_on)) AS agent_hired_on_month_max_session_id,
        ROW_NUMBER() OVER w_session_id_session_end_year AS session_end_seq,
        NTH_VALUE(duration_min, 2) OVER (PARTITION BY session_id, YEAR(session_end) ORDER BY session_end NULLS LAST, ticket_id, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_min,
        COUNT(*) OVER (PARTITION BY MONTH(agent_hired_on)) AS rows_in_agent_hired_on_month
    FROM enriched_ticket
    WINDOW w_agent_hired_on_month AS (PARTITION BY MONTH(agent_hired_on)), w_session_id_session_end_year AS (PARTITION BY session_id, YEAR(session_end) ORDER BY session_end NULLS LAST, ticket_id, session_id)
)
SELECT *
FROM (
    SELECT ticket_id, session_id, ticket_channel, duration_min
    FROM ranked
) src
PIVOT (
    MIN(duration_min)
    FOR ticket_channel IN ('email', 'phone')
)
ORDER BY ticket_id, session_id
