WITH filtered_agents AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        CONCAT(tier, '#', CAST(id AS STRING)) AS tier_ref,
        DATE_FORMAT(hired_on, 'MMM yyyy') AS hired_on_label,
        DATE_TRUNC('WEEK', hired_on) AS hired_on_week
    FROM support_agents
    WHERE LOWER(agent_name) LIKE '%is%'
      AND hired_on < DATE '2025-02-26'
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        agent_name,
        team,
        tier_ref,
        hired_on_label,
        hired_on_week,
        DAYOFWEEK(hired_on) AS hired_on_dayofweek
    FROM filtered_agents
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        agent_name,
        tier_ref,
        hired_on_week,
        hired_on_dayofweek,
        FIRST_VALUE(hired_on_week) IGNORE NULLS OVER (PARTITION BY hired_on_dayofweek ORDER BY hired_on_week, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_hired_on_week,
        ROUND(PERCENT_RANK() OVER (PARTITION BY team ORDER BY hired_on_dayofweek), 3) AS hired_on_dayofweek_percent_rank
    FROM scored
),
with_children_tickets AS (
    -- One row per ticket
    SELECT
        s.id,
        s.agent_name,
        s.tier_ref,
        s.hired_on_week,
        s.first_known_hired_on_week,
        s.hired_on_dayofweek_percent_rank,
        t.id AS ticket_id,
        t.priority AS ticket_priority,
        t.csat AS ticket_csat,
        t.closed_at AS ticket_closed_at
    FROM metrics s
    JOIN tickets t ON t.agent_id = s.id
)
SELECT
    ticket_priority,
    ticket_id,
    ticket_csat,
    ROUND(SUM(hired_on_dayofweek_percent_rank) / NULLIF(SUM(ticket_csat), 0), 3) AS hired_on_dayofweek_percent_rank_to_ticket_csat_r,
    COUNT(*) AS row_count
FROM with_children_tickets
GROUP BY CUBE(ticket_priority, ticket_id, ticket_csat)
ORDER BY ticket_priority NULLS LAST, ticket_id NULLS LAST, ticket_csat NULLS LAST
