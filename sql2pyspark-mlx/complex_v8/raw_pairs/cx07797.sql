WITH matched AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 7 DAYS)
),
combined_details_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM matched s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(hours_since_open) OVER w_ticket_id AS ticket_id_min_hours_since_open,
        MAX(hours_since_open) OVER (ORDER BY agent_hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_hours_since_open_so_far,
        LAST_VALUE(agent_hired_on) IGNORE NULLS OVER (ORDER BY hours_since_open, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_agent_hired_on,
        ROUND(CUME_DIST() OVER w_all_by_agent_hired_on, 3) AS agent_hired_on_cume_dist,
        NTILE(4) OVER w_all_by_agent_hired_on AS agent_hired_on_quartile
    FROM combined_details_agent
    WINDOW w_ticket_id AS (PARTITION BY ticket_id), w_all_by_agent_hired_on AS (ORDER BY agent_hired_on DESC, id)
),
latest_per_key AS (
    -- Keep the latest row per agent_tier
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        priority,
        agent_id,
        hours_since_open,
        agent_tier,
        agent_hired_on,
        agent_name,
        ticket_id_min_hours_since_open,
        max_hours_since_open_so_far,
        last_known_agent_hired_on,
        agent_hired_on_cume_dist,
        agent_hired_on_quartile
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_tier ORDER BY last_known_agent_hired_on DESC, id DESC) AS recency_rank
        FROM metrics
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    priority,
    agent_id,
    hours_since_open,
    agent_tier,
    agent_hired_on,
    agent_name,
    ticket_id_min_hours_since_open,
    max_hours_since_open_so_far,
    last_known_agent_hired_on,
    agent_hired_on_cume_dist,
    LENGTH(TRIM(agent_name)) AS agent_name_len,
    DATE_FORMAT(agent_hired_on, 'yyyy-MM-dd') AS agent_hired_on_label
FROM latest_per_key
ORDER BY max_hours_since_open_so_far DESC NULLS FIRST, last_known_agent_hired_on DESC NULLS FIRST
