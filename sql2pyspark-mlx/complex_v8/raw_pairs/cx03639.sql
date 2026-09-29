WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE actor = 'system'
      AND event_type <> 'replied'
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM base
    WHERE days_since_event_at > (SELECT AVG(days_since_event_at) FROM base)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(days_since_event_at, 1, 0) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at, id) AS prev_days_since_event_at,
        RANK() OVER (ORDER BY days_since_event_at ASC NULLS LAST) AS days_since_event_at_rank,
        DENSE_RANK() OVER (ORDER BY days_since_event_at ASC NULLS LAST) AS days_since_event_at_dense_rank
    FROM with_activity
),
with_parent_ticket AS (
    -- Attach ticket attributes
    SELECT s.*, t.csat AS ticket_csat, t.opened_at AS ticket_opened_at
    FROM metrics s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND t.csat BETWEEN 2 AND 4
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(days_since_event_at) OVER (PARTITION BY ticket_csat ORDER BY ticket_opened_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS days_since_event_at_avg_7,
        DENSE_RANK() OVER (PARTITION BY ticket_csat ORDER BY ticket_opened_at) AS ticket_opened_at_dense_rank
    FROM with_parent_ticket
),
leaders AS (
    -- First row per group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        days_since_event_at,
        prev_days_since_event_at,
        days_since_event_at_rank,
        days_since_event_at_dense_rank,
        ticket_csat,
        ticket_opened_at,
        days_since_event_at_avg_7
    FROM windowed
    WHERE ticket_opened_at_dense_rank = 1
)
SELECT id, field, reading
FROM (
    SELECT id, days_since_event_at, days_since_event_at_rank, ticket_csat
    FROM leaders
) src
UNPIVOT (
    reading FOR field IN (days_since_event_at, days_since_event_at_rank, ticket_csat)
) unp
