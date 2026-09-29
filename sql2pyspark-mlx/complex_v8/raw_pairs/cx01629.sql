WITH recent_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE (TO_DATE(event_at) < DATE '2025-02-24' OR event_type IS NOT NULL)
),
enriched AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        t.agent_id AS ticket_agent_id,
        t.requester_id AS ticket_requester_id,
        t.opened_at AS ticket_opened_at,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team
    FROM recent_events s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
    WHERE t.agent_id IS NOT NULL
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(ticket_requester_id) OVER (PARTITION BY ticket_id ORDER BY ticket_requester_id DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS ticket_requester_id_max_2,
        NTILE(4) OVER w_agent_hired_on_year_by_ticke AS ticket_opened_at_quartile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_id ORDER BY ticket_requester_id DESC, id), 3) AS ticket_requester_id_percent_rank,
        FIRST_VALUE(ticket_opened_at) OVER (PARTITION BY ticket_id ORDER BY ticket_requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ticket_opened_at,
        ROUND(ticket_requester_id / NULLIF(SUM(ticket_requester_id) OVER (PARTITION BY actor), 0), 4) AS ticket_requester_id_share_of_actor,
        NTH_VALUE(ticket_opened_at, 2) OVER (PARTITION BY event_type ORDER BY ticket_requester_id NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_opened_at,
        DENSE_RANK() OVER (PARTITION BY event_type ORDER BY ticket_requester_id NULLS LAST, id) AS ticket_requester_id_dense_rank,
        ROUND(ticket_requester_id - AVG(ticket_requester_id) OVER (PARTITION BY actor), 2) AS ticket_requester_id_vs_actor_avg
    FROM enriched
    WINDOW w_agent_hired_on_year_by_ticke AS (PARTITION BY YEAR(agent_hired_on) ORDER BY ticket_opened_at NULLS LAST, id)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(agent_team, '#', CAST(id AS STRING)) AS agent_team_ref,
        LOWER(REGEXP_REPLACE(TRIM(agent_team), ' ', '_')) AS agent_team_slug
    FROM with_windows
    WHERE ticket_requester_id_percent_rank < 0.8
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        ticket_requester_id,
        ticket_opened_at,
        agent_team,
        ticket_opened_at_quartile,
        ticket_requester_id_percent_rank,
        first_ticket_opened_at,
        ticket_requester_id_share_of_actor,
        ticket_requester_id_dense_rank,
        ticket_requester_id_vs_actor_avg,
        agent_team_ref,
        agent_team_slug,
        ROUND(ticket_requester_id - AVG(ticket_requester_id) OVER (PARTITION BY TO_DATE(second_ticket_opened_at), ticket_opened_at_quartile), 2) AS ticket_requester_id_vs_second_ticket_opened_at_t,
        COUNT(*) OVER w_second_ticket_opened_at_to_d AS rows_in_second_ticket_opened_at_to_date_ticket_o,
        MIN(ticket_requester_id_max_2) OVER (PARTITION BY ticket_opened_at_quartile, ticket_agent_id) AS ticket_opened_at_quartile_ticket_agent_id_min_ti
    FROM annotated
    WINDOW w_second_ticket_opened_at_to_d AS (PARTITION BY TO_DATE(second_ticket_opened_at), ticket_opened_at_quartile)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    ticket_requester_id,
    agent_team,
    ticket_opened_at_quartile,
    ticket_requester_id_percent_rank,
    first_ticket_opened_at,
    ticket_requester_id_share_of_actor,
    ticket_requester_id_dense_rank,
    ticket_requester_id_vs_actor_avg,
    agent_team_ref,
    agent_team_slug,
    ticket_requester_id_vs_second_ticket_opened_at_t,
    rows_in_second_ticket_opened_at_to_date_ticket_o,
    ticket_opened_at_quartile_ticket_agent_id_min_ti,
    COALESCE(agent_team, 'UNKNOWN') AS agent_team_or_unknown
FROM ranked
ORDER BY event_type, ticket_requester_id DESC NULLS FIRST
