WITH prepared_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags,
        csat - requester_id AS csat_minus_requester_id
    FROM tickets
    WHERE csat BETWEEN 1 AND 4
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(csat_minus_requester_id) OVER (PARTITION BY priority) AS priority_max_csat_minus_requester_id,
        ROUND(AVG(csat_minus_requester_id) OVER (PARTITION BY agent_id ORDER BY csat_minus_requester_id ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS csat_minus_requester_id_avg_1_prior,
        ROUND(AVG(csat) OVER (PARTITION BY priority), 2) AS priority_avg_csat
    FROM prepared_tickets
),
summary AS (
    -- Summarise per group
    SELECT
        agent_id,
        csat,
        COUNT(*) FILTER (WHERE priority IS NOT NULL) AS n_priority_is_not_null,
        ROUND(AVG(csat_minus_requester_id), 2) AS avg_csat_minus_requester_id
    FROM metrics
    GROUP BY agent_id, csat
    HAVING SUM(priority_max_csat_minus_requester_id) > 31
),
detail_rows_tickets AS (
    -- One row per ticket
    SELECT
        s.agent_id,
        s.csat,
        s.n_priority_is_not_null,
        s.avg_csat_minus_requester_id,
        t.id AS ticket_id,
        t.priority AS ticket_priority,
        t.closed_at AS ticket_closed_at,
        t.tags AS ticket_tags,
        t.channel AS ticket_channel
    FROM summary s
    LEFT JOIN tickets t ON t.agent_id = s.agent_id
)
-- Final projection
SELECT
    agent_id,
    n_priority_is_not_null,
    avg_csat_minus_requester_id,
    ticket_id,
    ticket_tags,
    ticket_channel
FROM detail_rows_tickets
ORDER BY ticket_channel
