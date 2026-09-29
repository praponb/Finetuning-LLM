WITH prepared_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-25 10:00:00'
      AND ticket_id >= 20
),
with_parent_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.requester_id AS ticket_requester_id,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM prepared_events s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(ticket_requester_id) IGNORE NULLS OVER (PARTITION BY agent_team, event_type ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ticket_requester_id,
        LAST_VALUE(agent_hired_on) OVER (ORDER BY agent_hired_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_agent_hired_on,
        COUNT(*) OVER w_event_type_event_at_year AS rows_in_event_type_event_at_year,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id, DATE_TRUNC('MONTH', event_at) ORDER BY agent_hired_on, id), 3) AS agent_hired_on_cume_dist,
        ROUND(ticket_requester_id / NULLIF(SUM(ticket_requester_id) OVER (PARTITION BY event_type, YEAR(event_at)), 0), 4) AS ticket_requester_id_share_of_event_type_event_at,
        MAX(ticket_requester_id) OVER w_event_type_event_at_year AS event_type_event_at_year_max_ticket_requester_id,
        ROUND(AVG(ticket_requester_id) OVER (ORDER BY agent_hired_on ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS ticket_requester_id_avg_7,
        MAX(ticket_requester_id) OVER (ORDER BY agent_hired_on ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS ticket_requester_id_max_6_prior,
        NTH_VALUE(ticket_requester_id, 2) OVER (PARTITION BY ticket_id, DATE_TRUNC('MONTH', event_at) ORDER BY agent_hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_requester_id,
        LAST_VALUE(event_at) OVER (PARTITION BY ticket_id, DATE_TRUNC('MONTH', event_at) ORDER BY agent_hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_team, event_type ORDER BY event_at NULLS LAST), 3) AS event_at_cume_dist,
        ROUND(ticket_requester_id - AVG(ticket_requester_id) OVER (PARTITION BY event_type, YEAR(event_at)), 2) AS ticket_requester_id_vs_event_type_event_at_year_,
        ROUND(CUME_DIST() OVER (ORDER BY agent_hired_on ASC, id), 3) AS agent_hired_on_cume_dist_2,
        DENSE_RANK() OVER (PARTITION BY agent_team, event_type ORDER BY event_at NULLS LAST) AS event_at_dense_rank,
        FIRST_VALUE(event_at) OVER (PARTITION BY agent_team, event_type ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        NTILE(10) OVER (PARTITION BY ticket_id, DATE_TRUNC('MONTH', event_at) ORDER BY agent_hired_on, id) AS agent_hired_on_decile
    FROM with_parent_ticket
    WINDOW w_event_type_event_at_year AS (PARTITION BY event_type, YEAR(event_at))
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM ranked
    WHERE event_at_dense_rank <= 5
)
SELECT
    id,
    event_at,
    actor,
    agent_team,
    agent_hired_on,
    last_known_ticket_requester_id,
    last_agent_hired_on,
    rows_in_event_type_event_at_year,
    agent_hired_on_cume_dist,
    event_type_event_at_year_max_ticket_requester_id,
    ticket_requester_id_avg_7,
    ticket_requester_id_max_6_prior,
    last_event_at,
    ticket_requester_id_vs_event_type_event_at_year_,
    agent_hired_on_cume_dist_2,
    first_event_at,
    agent_hired_on_decile
FROM first_rows
ORDER BY id
