WITH recent_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        INITCAP(TRIM(event_type)) AS event_type_clean,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-03-08 09:00:00'
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(*) FILTER (WHERE actor NOT IN ('system', 'customer')) AS event_actor_count
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_actor_count, 0) AS event_actor_count
    FROM recent_events s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
joined AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.event_type_clean,
        s.actor_or_unknown,
        s.event_actor_count,
        t.channel AS ticket_channel,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team
    FROM with_stats s
    JOIN tickets t ON t.id = s.ticket_id AND t.priority = 'P2'
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY actor_or_unknown)) AS distinct_actor_in_actor_or_unknown,
        MAX(event_actor_count) OVER (PARTITION BY ticket_channel, actor ORDER BY agent_hired_on DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS event_actor_count_max_3,
        SIZE(COLLECT_SET(ticket_channel) OVER (PARTITION BY actor_or_unknown)) AS distinct_ticket_channel_in_actor_or_unknown,
        COUNT(*) OVER w_actor_or_unknown AS rows_in_actor_or_unknown,
        SUM(event_actor_count) OVER w_actor_or_unknown AS actor_or_unknown_sum_event_actor_count
    FROM joined
    WINDOW w_actor_or_unknown AS (PARTITION BY actor_or_unknown)
),
ticket_id_stats AS (
    -- Max actor_or_unknown_sum_event_actor_count per ticket_id
    SELECT ticket_id, MAX(actor_or_unknown_sum_event_actor_count) AS max_actor_or_unknown_sum_event_actor_count
    FROM with_windows
    GROUP BY ticket_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_actor_or_unknown_sum_event_actor_count,
        ROUND(s.actor_or_unknown_sum_event_actor_count - g.max_actor_or_unknown_sum_event_actor_count, 2) AS actor_or_unknown_sum_event_actor_count_minus_max
    FROM with_windows s
    JOIN ticket_id_stats g ON g.ticket_id = s.ticket_id
    WHERE s.actor_or_unknown_sum_event_actor_count = g.max_actor_or_unknown_sum_event_actor_count
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(agent_hired_on, 2) OVER (PARTITION BY actor, distinct_ticket_channel_in_actor_or_unknown ORDER BY rows_in_actor_or_unknown DESC, id) AS next2_agent_hired_on,
        DENSE_RANK() OVER (ORDER BY distinct_ticket_channel_in_actor_or_unknown) AS distinct_ticket_channel_in_actor_or_unknown_dens,
        COUNT(*) OVER (PARTITION BY actor, distinct_ticket_channel_in_actor_or_unknown ORDER BY rows_in_actor_or_unknown DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM above_average
)
-- Final projection
SELECT
    id,
    ticket_id,
    actor,
    event_type_clean,
    actor_or_unknown,
    event_actor_count,
    ticket_channel,
    agent_team,
    distinct_actor_in_actor_or_unknown,
    event_actor_count_max_3,
    distinct_ticket_channel_in_actor_or_unknown,
    rows_in_actor_or_unknown,
    actor_or_unknown_sum_event_actor_count,
    max_actor_or_unknown_sum_event_actor_count,
    actor_or_unknown_sum_event_actor_count_minus_max,
    next2_agent_hired_on,
    running_count,
    CONCAT(actor, '#', CAST(ticket_id AS STRING)) AS actor_ref
FROM metrics
