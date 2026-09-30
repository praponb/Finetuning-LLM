WITH recent_agents AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        tier,
        hired_on,
        CONCAT_WS('|', tier, team) AS tier_team_label,
        DATE_FORMAT(hired_on, 'MMM yyyy') AS hired_on_label
    FROM support_agents
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MIN(t.opened_at) FROM tickets t WHERE t.agent_id = s.id) AS first_ticket_opened_at,
        (SELECT MIN(t.opened_at) FROM tickets t WHERE t.agent_id = s.id) AS first_ticket_opened_at_2
    FROM recent_agents s
),
exploded_detail_tickets AS (
    -- One row per ticket
    SELECT
        s.id,
        s.hired_on_label,
        s.first_ticket_opened_at,
        s.first_ticket_opened_at_2,
        t.id AS ticket_id,
        t.closed_at AS ticket_closed_at,
        t.csat AS ticket_csat,
        t.requester_id AS ticket_requester_id
    FROM with_stats s
    LEFT JOIN tickets t ON t.agent_id = s.id
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        ABS(ticket_csat - 4) AS ticket_csat_distance,
        LOWER(TRIM(hired_on_label)) AS hired_on_label_clean
    FROM exploded_detail_tickets
),
line_level_tickets AS (
    -- One row per ticket
    SELECT
        s.id,
        s.hired_on_label,
        s.ticket_id,
        s.ticket_csat,
        s.ticket_csat_distance,
        t.id AS ticket_id_2,
        t.closed_at AS ticket_closed_at_2,
        t.priority AS ticket_priority
    FROM derived s
    JOIN tickets t ON t.agent_id = s.id
)
SELECT
    id,
    hired_on_label,
    ticket_id,
    ticket_csat,
    ticket_csat_distance,
    ticket_id_2,
    ticket_closed_at_2,
    ticket_priority,
    SUBSTRING(TRIM(ticket_priority), 1, 3) AS ticket_priority_prefix,
    ticket_csat + ticket_csat_distance AS ticket_csat_plus_ticket_csat_distance
FROM line_level_tickets
WHERE ticket_id >= 44
ORDER BY ticket_csat_distance DESC
