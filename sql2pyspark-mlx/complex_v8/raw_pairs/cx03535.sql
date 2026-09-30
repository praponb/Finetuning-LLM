WITH cleaned_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        INITCAP(TRIM(actor)) AS actor_clean
    FROM ticket_events
    WHERE event_type = 'assigned'
      AND MONTH(event_at) IN (2, 3)
),
joined AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.csat AS ticket_csat,
        t.opened_at AS ticket_opened_at
    FROM cleaned_events s
    JOIN tickets t ON t.id = s.ticket_id
),
totals AS (
    -- Summarise per group
    SELECT
        ticket_csat,
        YEAR(event_at) AS event_at_year,
        ticket_id,
        COUNT(DISTINCT actor) AS distinct_actors,
        COUNT(*) AS row_count,
        ROUND(AVG(ticket_csat), 2) AS avg_ticket_csat,
        SUM(CASE WHEN event_type = 'escalated' THEN 1 ELSE 0 END) AS n_event_type_escalated
    FROM joined
    GROUP BY 1, 2, 3
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(avg_ticket_csat * 1.5, 2) AS avg_ticket_csat_adj,
        ABS(n_event_type_escalated - 5) AS n_event_type_escalated_distance
    FROM totals
    WHERE distinct_actors BETWEEN 2 AND 8
),
filtered AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM tagged s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type NOT IN ('assigned', 'escalated')
              AND t.actor IS NOT NULL
        )
)
-- Final projection
SELECT
    ticket_csat,
    row_count,
    avg_ticket_csat,
    n_event_type_escalated,
    n_event_type_escalated_distance,
    CASE WHEN row_count >= 7 THEN 'A' WHEN row_count >= 4 THEN 'B' ELSE 'C' END AS row_count_tier,
    ABS(ticket_csat - 4) AS ticket_csat_distance
FROM filtered
ORDER BY row_count NULLS LAST, n_event_type_escalated_distance DESC
