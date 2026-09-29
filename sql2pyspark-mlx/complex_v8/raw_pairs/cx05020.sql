WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT *
    FROM ticket_events
    WHERE event_type = 'replied'
),
with_parent AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.csat AS ticket_csat,
        su.team AS agent_team
    FROM filtered s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
derived AS (
    -- Derive helper columns
    SELECT *, LENGTH(TRIM(actor)) AS actor_len
    FROM with_parent
    WHERE event_type = 'resolved'
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        UPPER(TRIM(agent_team)) AS agent_team_clean,
        LEAST(ticket_csat, actor_len) AS least_ticket_csat_actor_len
    FROM derived
)
-- Subtotals with GROUPING SETS
SELECT
    ticket_id,
    ticket_csat,
    ROUND(SUM(ticket_csat) / NULLIF(SUM(actor_len), 0), 3) AS ticket_csat_to_actor_len_ratio,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT agent_team) AS distinct_agent_teams
FROM annotated
GROUP BY GROUPING SETS ((ticket_id, ticket_csat), (ticket_id), ())
ORDER BY ticket_id NULLS LAST, ticket_csat NULLS LAST
