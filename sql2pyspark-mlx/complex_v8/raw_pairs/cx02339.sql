WITH filtered_agents AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        hired_on,
        CONCAT_WS('-', agent_name, team) AS agent_name_team_label,
        COALESCE(team, 'UNKNOWN') AS team_or_unknown,
        DATE_FORMAT(hired_on, 'yyyy-MM') AS hired_on_label
    FROM support_agents
),
exploded_detail AS (
    -- One row per ticket
    SELECT
        s.id,
        s.hired_on_label,
        t.id AS ticket_id,
        t.requester_id AS ticket_requester_id,
        t.csat AS ticket_csat
    FROM filtered_agents s
    JOIN tickets t ON t.agent_id = s.id
),
with_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.closed_at AS ticket_closed_at,
        t.agent_id AS ticket_agent_id,
        t.opened_at AS ticket_opened_at,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM exploded_detail s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
)
-- Subtotals with GROUPING SETS
SELECT
    ticket_csat,
    ticket_agent_id,
    ticket_id,
    GROUPING(ticket_csat) AS is_total_ticket_csat,
    SUM(ticket_requester_id) AS total_ticket_requester_id,
    COUNT(*) AS n_rows
FROM with_details_ticket
GROUP BY GROUPING SETS ((ticket_csat, ticket_agent_id, ticket_id), (ticket_csat), ())
ORDER BY ticket_csat NULLS LAST, ticket_agent_id NULLS LAST, ticket_id NULLS LAST
