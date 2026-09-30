WITH prepared_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label,
        CONCAT_WS('-', event_type, actor) AS event_type_actor_label
    FROM ticket_events
    WHERE ticket_id >= 45
),
with_parent_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.event_at_label,
        s.event_type_actor_label,
        t.opened_at AS ticket_opened_at,
        t.tags AS ticket_tags,
        t.priority AS ticket_priority,
        su.team AS agent_team,
        su.agent_name
    FROM prepared_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY YEAR(event_at) ORDER BY ticket_opened_at ASC, id) AS ticket_opened_at_rank,
        LAG(event_at) OVER w_event_at_year_by_ticket_open AS prev_event_at,
        LEAD(ticket_opened_at, 3) OVER (ORDER BY event_at, id) AS next3_ticket_opened_at,
        LAG(event_at) OVER (ORDER BY event_at, id) AS prev_event_at_2,
        LEAD(event_at) OVER (PARTITION BY YEAR(event_at) ORDER BY ticket_opened_at ASC, id) AS next_event_at,
        RANK() OVER w_event_type_event_at_to_date AS event_at_rank,
        LAG(ticket_opened_at, 2) OVER w_all_by_ticket_opened_at AS prev2_ticket_opened_at,
        NTILE(10) OVER w_all_by_ticket_opened_at AS ticket_opened_at_decile
    FROM with_parent_ticket
    WINDOW w_event_at_year_by_ticket_open AS (PARTITION BY YEAR(event_at) ORDER BY ticket_opened_at ASC, id), w_event_type_event_at_to_date AS (PARTITION BY event_type, TO_DATE(event_at) ORDER BY event_at ASC NULLS LAST), w_all_by_ticket_opened_at AS (ORDER BY ticket_opened_at DESC, id)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        agent_team,
        COUNT(DISTINCT agent_team) AS distinct_agent_teams,
        SUM(ticket_opened_at_decile) AS total_ticket_opened_at_decile,
        MIN(prev2_ticket_opened_at) AS first_prev2_ticket_opened_at,
        COUNT(*) AS cnt
    FROM with_trends
    GROUP BY agent_team
)
SELECT
    agent_team,
    distinct_agent_teams,
    total_ticket_opened_at_decile,
    cnt,
    LENGTH(TRIM(agent_team)) AS agent_team_len
FROM stats
WHERE total_ticket_opened_at_decile > 19
ORDER BY cnt
