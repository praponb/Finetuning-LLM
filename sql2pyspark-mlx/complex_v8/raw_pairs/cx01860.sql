WITH valid_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        csat,
        tags,
        QUARTER(opened_at) AS opened_at_quarter,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 60, 1) AS minutes_opened_at_to_closed_at
    FROM tickets
    WHERE agent_id IS NOT NULL
      AND closed_at >= TIMESTAMP '2025-01-12 08:00:00'
),
exploded AS (
    -- One row per tag
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.csat,
        s.opened_at_quarter,
        s.minutes_opened_at_to_closed_at,
        t.tag_pos,
        t.tag
    FROM valid_tickets s
    LATERAL VIEW OUTER POSEXPLODE(s.tags) t AS tag_pos, tag
)
SELECT id, tag_pos, metric, amount
FROM (
    SELECT id, tag_pos, requester_id, opened_at_quarter, csat
    FROM exploded
) src
UNPIVOT (
    amount FOR metric IN (requester_id, opened_at_quarter, csat)
) unp
