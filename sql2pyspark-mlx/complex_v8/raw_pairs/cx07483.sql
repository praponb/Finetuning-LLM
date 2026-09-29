WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.csat) AS prior_csat_1d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        COUNT(*) AS ticket_count,
        MAX(opened_at) AS last_ticket_opened_at
    FROM tickets
    GROUP BY agent_id
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count,
        t.last_ticket_opened_at
    FROM matched s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
)
SELECT id, metric, reading
FROM (
    SELECT id, prior_csat_1d, ticket_count, prior_rows_1d
    FROM profiled
) src
UNPIVOT (
    reading FOR metric IN (prior_csat_1d, ticket_count, prior_rows_1d)
) unp
ORDER BY id, metric
