WITH cleaned AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        CASE WHEN channel IN ('phone', 'email') THEN 'group_a' WHEN channel = 'web' THEN 'group_b' ELSE 'other' END AS channel_group,
        ABS(requester_id - 13) AS requester_id_distance
    FROM tickets
    WHERE priority = 'P4'
      AND csat <= 4
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN t.event_type = 'escalated' THEN 1 END) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_event_type_count,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at
    FROM cleaned s
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.channel,
        s.requester_id_distance,
        s.event_event_type_count,
        t.id AS event_id,
        t.event_type,
        t.actor AS event_actor
    FROM with_stats s
    JOIN ticket_events t ON t.ticket_id = s.id
),
with_parent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.channel,
        s.requester_id_distance,
        s.event_id,
        s.event_type,
        s.event_actor,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM with_children_events s
    JOIN support_agents su ON su.id = s.agent_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        agent_id,
        channel,
        event_id,
        event_type,
        agent_team,
        agent_hired_on,
        ROUND(requester_id_distance - AVG(requester_id_distance) OVER (PARTITION BY event_actor), 2) AS requester_id_distance_vs_event_actor_avg,
        COUNT(*) OVER (PARTITION BY event_actor) AS rows_in_event_actor,
        ROUND(CUME_DIST() OVER (PARTITION BY channel, event_id ORDER BY agent_hired_on DESC, id, event_id), 3) AS agent_hired_on_cume_dist,
        SUM(requester_id_distance) OVER (PARTITION BY channel, event_id ORDER BY agent_hired_on DESC, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id_distance,
        SUM(requester_id_distance) OVER w_event_actor AS event_actor_sum_requester_id_distance,
        ROUND(AVG(requester_id_distance) OVER (PARTITION BY channel, event_id ORDER BY agent_hired_on DESC, id, event_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS requester_id_distance_avg_3_prior
    FROM with_parent
    WINDOW w_event_actor AS (PARTITION BY event_actor)
),
combined_details_event AS (
    -- Attach event attributes
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.event_id,
        s.agent_team,
        s.agent_hired_on,
        s.rows_in_event_actor,
        s.agent_hired_on_cume_dist,
        s.running_requester_id_distance,
        s.event_actor_sum_requester_id_distance,
        t.ticket_id AS event_ticket_id,
        t.actor AS event_actor,
        t.event_at
    FROM with_windows s
    LEFT JOIN ticket_events t ON t.id = s.event_id
),
enriched AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.event_id,
        s.agent_team,
        s.agent_hired_on,
        s.running_requester_id_distance,
        s.event_actor_sum_requester_id_distance,
        s.event_ticket_id,
        s.event_actor,
        t.closed_at AS ticket_closed_at
    FROM combined_details_event s
    LEFT JOIN tickets t ON t.id = s.event_ticket_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, event_id, field, reading
FROM (
    SELECT id, event_id, event_actor_sum_requester_id_distance, running_requester_id_distance
    FROM enriched
) src
UNPIVOT INCLUDE NULLS (
    reading FOR field IN (event_actor_sum_requester_id_distance, running_requester_id_distance)
) unp
ORDER BY id, event_id, field
