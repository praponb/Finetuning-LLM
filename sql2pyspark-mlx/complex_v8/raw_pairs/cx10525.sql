WITH scoped_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        channel,
        csat,
        tags
    FROM tickets
    WHERE csat < 4
),
agent_id_stats AS (
    -- Max csat per agent_id
    SELECT agent_id, MAX(csat) AS max_csat
    FROM scoped_tickets
    GROUP BY agent_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_csat, ROUND(s.csat - g.max_csat, 2) AS csat_minus_max
    FROM scoped_tickets s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.csat = g.max_csat
),
without_matches AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM outliers s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.actor IS NOT NULL
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, reading
FROM (
    SELECT id, max_csat, csat_minus_max
    FROM without_matches
) src
UNPIVOT INCLUDE NULLS (
    reading FOR field IN (max_csat, csat_minus_max)
) unp
