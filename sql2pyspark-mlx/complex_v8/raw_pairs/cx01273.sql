WITH cleaned_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM tickets
    WHERE TO_DATE(opened_at) < DATE '2025-02-12'
),
joined AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        su.hired_on AS agent_hired_on
    FROM cleaned_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        closed_at + INTERVAL 2 HOURS AS closed_at_plus_2h,
        DATE_TRUNC('WEEK', agent_hired_on) AS agent_hired_on_week,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug
    FROM joined
    WHERE requester_id BETWEEN 7 AND 25
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.priority,
        s.agent_hired_on_week,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type
    FROM enriched s
    JOIN ticket_events t ON t.ticket_id = s.id AND t.event_type = 'escalated'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        priority,
        agent_hired_on_week,
        event_id,
        RANK() OVER w_event_id_by_agent_hired_on_w AS agent_hired_on_week_rank,
        SIZE(COLLECT_SET(event_actor) OVER (PARTITION BY event_type)) AS distinct_event_actor_in_event_type,
        MAX(requester_id) OVER (PARTITION BY event_id ORDER BY agent_hired_on_week DESC, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_requester_id_so_far,
        SUM(requester_id) OVER (PARTITION BY event_id ORDER BY agent_hired_on_week DESC, id, event_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS requester_id_sum_3,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY event_type), 0), 4) AS requester_id_share_of_event_type,
        MAX(requester_id) OVER (PARTITION BY event_type) AS event_type_max_requester_id
    FROM with_children_events
    WINDOW w_event_id_by_agent_hired_on_w AS (PARTITION BY event_id ORDER BY agent_hired_on_week DESC, id, event_id)
),
with_windows_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(requester_id_sum_3, 2) OVER w_event_id_by_event_type_max_r AS prev2_requester_id_sum_3,
        SIZE(COLLECT_SET(event_id) OVER (PARTITION BY distinct_event_actor_in_event_type)) AS distinct_event_id_in_distinct_event_actor_in_eve
    FROM with_windows
    WINDOW w_event_id_by_event_type_max_r AS (PARTITION BY event_id ORDER BY event_type_max_requester_id ASC, id, event_id)
)
SELECT
    id,
    priority,
    agent_hired_on_week,
    agent_hired_on_week_rank,
    distinct_event_actor_in_event_type,
    max_requester_id_so_far,
    requester_id_sum_3,
    requester_id_share_of_event_type,
    distinct_event_id_in_distinct_event_actor_in_eve
FROM with_windows_2
