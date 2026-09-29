WITH recent AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(actor, '#', CAST(ticket_id AS STRING)) AS actor_ref
    FROM ticket_events
    WHERE actor IN ('agent', 'system')
),
with_details_ticket AS (
    -- Join tickets
    SELECT s.*, t.csat AS ticket_csat
    FROM recent s
    JOIN tickets t ON t.id = s.ticket_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        actor_ref,
        ticket_csat,
        COUNT(*) OVER w_event_type_event_at_month AS rows_in_event_type_event_at_month,
        RANK() OVER w_event_type_by_event_at_ties AS event_at_rank
    FROM with_details_ticket
    WINDOW w_event_type_event_at_month AS (PARTITION BY event_type, DATE_TRUNC('MONTH', event_at)), w_event_type_by_event_at_ties AS (PARTITION BY event_type ORDER BY event_at ASC NULLS LAST)
),
actor_stats AS (
    -- Max event_at_rank per actor
    SELECT actor, MAX(event_at_rank) AS max_event_at_rank
    FROM metrics
    GROUP BY actor
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_event_at_rank,
        ROUND(s.event_at_rank - g.max_event_at_rank, 2) AS event_at_rank_minus_max
    FROM metrics s
    JOIN actor_stats g ON g.actor = s.actor
    WHERE s.event_at_rank = g.max_event_at_rank
)
-- Subtotals with CUBE
SELECT
    COALESCE(actor, 'ALL') AS actor,
    ticket_csat,
    COUNT(*) AS row_count,
    COUNT(DISTINCT actor) AS distinct_actors,
    COUNT(*) FILTER (WHERE actor = 'customer') AS n_actor_customer
FROM compared
GROUP BY CUBE(actor, ticket_csat)
ORDER BY actor NULLS LAST, ticket_csat NULLS LAST
