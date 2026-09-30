WITH valid_agents AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        QUARTER(hired_on) AS hired_on_quarter,
        DATE_FORMAT(hired_on, 'MMM yyyy') AS hired_on_label
    FROM support_agents
    WHERE YEAR(hired_on) = 2024
      AND team = 'Billing'
),
exploded_detail_tickets AS (
    -- One row per ticket
    SELECT
        s.id,
        s.hired_on_quarter,
        s.hired_on_label,
        t.id AS ticket_id,
        t.tags AS ticket_tags,
        t.channel AS ticket_channel,
        t.csat AS ticket_csat
    FROM valid_agents s
    JOIN tickets t ON t.agent_id = s.id
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        hired_on_label,
        ticket_id,
        ticket_tags,
        ticket_channel,
        ROUND(hired_on_quarter / NULLIF(ticket_csat, 0), 3) AS hired_on_quarter_per_ticket_csat,
        COALESCE(ticket_csat, 0) AS ticket_csat_filled,
        COALESCE(ticket_csat, 0) AS ticket_csat_filled_2
    FROM exploded_detail_tickets
),
joined AS (
    -- Join tickets
    SELECT
        s.id,
        s.hired_on_label,
        s.ticket_id,
        s.ticket_tags,
        s.ticket_channel,
        s.ticket_csat_filled,
        t.agent_id AS ticket_agent_id,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM derived s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
)
SELECT
    ticket_agent_id,
    ticket_csat_filled,
    GROUPING(ticket_agent_id) AS is_total_ticket_agent_id,
    COUNT(DISTINCT ticket_channel) AS distinct_ticket_channels
FROM joined
GROUP BY GROUPING SETS ((ticket_agent_id, ticket_csat_filled), (ticket_agent_id), ())
