WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('YEAR', event_at) AS event_at_year
    FROM ticket_events
    WHERE event_type = 'assigned'
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown,
        YEAR(event_at_year) AS event_at_year_year
    FROM prepared
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(event_at_year_year - AVG(event_at_year_year) OVER (PARTITION BY ticket_id), 2) AS event_at_year_year_vs_ticket_id_avg,
        FIRST_VALUE(event_at_year_year) IGNORE NULLS OVER (PARTITION BY actor_or_unknown, ticket_id ORDER BY event_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_year_year,
        LEAD(event_at_year_year, 3) OVER (PARTITION BY actor_or_unknown, ticket_id ORDER BY event_at ASC NULLS LAST, id) AS next3_event_at_year_year
    FROM tagged
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(event_at) OVER (PARTITION BY event_at_year_year ORDER BY event_at NULLS LAST, id) AS prev_event_at,
        RANK() OVER (ORDER BY event_at DESC) AS event_at_rank,
        COUNT(*) OVER w_event_at_year_month AS rows_in_event_at_year_month,
        FIRST_VALUE(event_at_year_year) IGNORE NULLS OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_year_year_2,
        SUM(event_at_year_year) OVER (PARTITION BY event_at_year_year ORDER BY event_at NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS event_at_year_year_sum_3
    FROM metrics
    WINDOW w_event_at_year_month AS (PARTITION BY DATE_TRUNC('MONTH', event_at_year))
),
qualified AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM with_trends s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND HOUR(t.opened_at) BETWEEN 8 AND 13
              AND t.channel NOT IN ('phone', 'chat')
        )
),
with_parent AS (
    -- Join tickets
    SELECT s.*, t.opened_at AS ticket_opened_at
    FROM qualified s
    JOIN tickets t ON t.id = s.ticket_id
)
SELECT *
FROM (
    SELECT first_known_event_at_year_year_2, event_at_year, actor, event_at_year_year_sum_3
    FROM with_parent
) src
PIVOT (
    AVG(event_at_year_year_sum_3)
    FOR actor IN ('agent', 'customer')
)
ORDER BY first_known_event_at_year_year_2, event_at_year
