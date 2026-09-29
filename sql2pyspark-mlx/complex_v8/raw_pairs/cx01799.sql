WITH base AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 3600, 1) AS hours_opened_at_to_closed_at
    FROM tickets
    WHERE channel NOT IN ('chat', 'web')
      AND priority IN ('P4', 'P2', 'P1')
),
filtered AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.csat >= 2
              AND t.channel = 'email'
        )
      AND s.csat < 4
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        opened_at + INTERVAL 30 MINUTES AS opened_at_plus_30m,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(closed_at)) / 60, 1) AS minutes_closed_at_to_opened_at,
        closed_at + INTERVAL 1 DAYS AS closed_at_plus_1d
    FROM filtered
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    closed_at,
    priority,
    csat,
    tags,
    hours_opened_at_to_closed_at,
    opened_at_plus_30m,
    minutes_closed_at_to_opened_at
FROM tagged
WHERE hours_opened_at_to_closed_at < 400.0
