WITH cleaned AS (
    -- Scope the support_agents rows
    SELECT
        id,
        team,
        tier,
        hired_on,
        TRUNC(hired_on, 'MM') AS hired_on_month_start,
        LOWER(REGEXP_REPLACE(TRIM(agent_name), ' ', '_')) AS agent_name_slug
    FROM support_agents
    WHERE hired_on < DATE '2025-02-26'
      AND team IN ('Onboarding', 'VIP')
),
line_level_tickets AS (
    -- One row per ticket
    SELECT
        s.id,
        s.team,
        s.tier,
        s.hired_on,
        s.hired_on_month_start,
        t.id AS ticket_id,
        t.requester_id AS ticket_requester_id,
        t.tags AS ticket_tags,
        t.closed_at AS ticket_closed_at,
        t.priority AS ticket_priority
    FROM cleaned s
    JOIN tickets t ON t.agent_id = s.id AND t.closed_at < TIMESTAMP '2025-02-15 09:00:00'
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM line_level_tickets
    WHERE ticket_requester_id > (SELECT MIN(ticket_requester_id) FROM line_level_tickets)
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        team,
        tier,
        hired_on,
        hired_on_month_start,
        ticket_id,
        ticket_requester_id,
        ticket_tags,
        ticket_priority,
        COALESCE(ticket_requester_id, 0) AS ticket_requester_id_filled
    FROM screened
    WHERE tier = 'T1'
)
SELECT
    id,
    team,
    hired_on,
    hired_on_month_start,
    ticket_requester_id,
    ticket_priority,
    ticket_requester_id_filled,
    CONCAT_WS(' / ', ticket_priority, tier) AS ticket_priority_tier_label
FROM derived
ORDER BY id NULLS LAST, hired_on NULLS LAST
