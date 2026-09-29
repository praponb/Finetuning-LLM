WITH prepared_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        csat,
        tags
    FROM tickets
    WHERE SIZE(tags) >= 1
),
without_matches AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM prepared_tickets s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.opened_at >= TIMESTAMP '2025-02-06 11:00:00'
              AND t.channel IN ('phone', 'web')
        )
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, MIN(csat) AS min_ticket_csat, COUNT(*) AS ticket_count
    FROM tickets
    GROUP BY agent_id
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.min_ticket_csat,
        COALESCE(t.ticket_count, 0) AS ticket_count
    FROM without_matches s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        agent_id,
        tags,
        LEAD(min_ticket_csat) OVER (PARTITION BY agent_id, min_ticket_csat ORDER BY min_ticket_csat ASC, id) AS next_min_ticket_csat,
        NTH_VALUE(csat, 2) OVER (PARTITION BY agent_id, min_ticket_csat ORDER BY min_ticket_csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat
    FROM summarized_rows
)
SELECT id, field, value
FROM (
    SELECT id, requester_id, second_csat, next_min_ticket_csat
    FROM with_trends
) src
UNPIVOT (
    value FOR field IN (requester_id, second_csat, next_min_ticket_csat)
) unp
ORDER BY id, field
