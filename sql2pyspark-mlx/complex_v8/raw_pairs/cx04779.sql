WITH filtered_agents AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        hired_on,
        CASE WHEN team IN ('VIP', 'Tech') THEN 'group_a' WHEN team = 'Billing' THEN 'group_b' ELSE 'other' END AS team_group,
        LENGTH(TRIM(agent_name)) AS agent_name_len,
        DATEDIFF(DATE '2026-01-01', hired_on) AS days_since_hired_on
    FROM support_agents
    WHERE tier NOT IN ('T2', 'T3')
      AND hired_on < DATE '2025-02-27'
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        agent_name,
        hired_on,
        team_group,
        agent_name_len,
        days_since_hired_on,
        CASE WHEN team IN ('Onboarding', 'Billing') THEN 'group_a' WHEN team = 'Tech' THEN 'group_b' ELSE 'other' END AS team_group_2
    FROM filtered_agents
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.id) AS ticket_count,
        (SELECT MAX(t.csat) FROM tickets t WHERE t.agent_id = s.id) AS max_ticket_csat,
        (SELECT SUM(t.requester_id) FROM tickets t WHERE t.agent_id = s.id) AS total_ticket_requester_id
    FROM tagged s
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM profiled
    WHERE agent_name_len > (SELECT AVG(agent_name_len) FROM profiled)
),
screened AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM without_matches s
    WHERE s.id IN (
            SELECT t.agent_id
            FROM tickets t
            WHERE t.priority NOT IN ('P4', 'P1')
              AND t.closed_at >= TIMESTAMP '2025-02-02 05:00:00'
        )
),
ticket_stats_2 AS (
    -- Per-agent tickets metrics
    SELECT agent_id, COUNT(*) AS ticket_count_2
    FROM tickets
    GROUP BY agent_id
),
profiled_2 AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count_2, 0) AS ticket_count_2
    FROM screened s
    LEFT JOIN ticket_stats_2 t ON t.agent_id = s.id
)
-- Final projection
SELECT
    id,
    hired_on,
    team_group,
    agent_name_len,
    team_group_2,
    ticket_count,
    max_ticket_csat,
    total_ticket_requester_id,
    ticket_count_2
FROM profiled_2
ORDER BY hired_on NULLS LAST, id
LIMIT 20
