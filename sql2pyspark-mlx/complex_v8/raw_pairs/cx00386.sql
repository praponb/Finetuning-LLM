WITH scoped AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        csat,
        DATEDIFF(DATE '2025-06-30', closed_at) AS days_since_closed_at
    FROM tickets
    WHERE priority = 'P3'
      AND channel NOT IN ('phone', 'web')
),
latest AS (
    -- Keep the latest row per agent_id
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        csat,
        days_since_closed_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY requester_id DESC, id DESC) AS rn
        FROM scoped
    ) ranked
    WHERE rn = 1
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 60, 1) AS minutes_opened_at_to_closed_at,
        COALESCE(days_since_closed_at, 0) AS days_since_closed_at_filled
    FROM latest
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM annotated s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_at >= TIMESTAMP '2025-01-25 07:00:00'
              AND t.actor = 'system'
        )
)
-- Final projection
SELECT DISTINCT closed_at, days_since_closed_at_filled
FROM with_activity
