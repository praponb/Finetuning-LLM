WITH base_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        tags,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown,
        GREATEST(requester_id, csat) AS greatest_requester_id_csat
    FROM tickets
    WHERE opened_at >= TIMESTAMP '2025-01-25 11:00:00'
      AND requester_id BETWEEN 7 AND 17
),
enriched AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.tags,
        s.csat_per_requester_id,
        s.priority_or_unknown,
        s.greatest_requester_id_csat,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM base_tickets s
    JOIN support_agents su ON su.id = s.agent_id
)
-- Subtotals with GROUPING SETS
SELECT
    agent_id,
    COALESCE(priority_or_unknown, 'ALL') AS priority_or_unknown,
    ROUND(100 * COUNT(CASE WHEN channel <> 'phone' THEN 1 END) / COUNT(*), 1) AS pct_channel_phone,
    SUM(greatest_requester_id_csat) AS total_greatest_requester_id_csat,
    COUNT(*) FILTER (WHERE channel = 'email') AS n_channel_email
FROM enriched
GROUP BY GROUPING SETS ((agent_id, priority_or_unknown), (agent_id), ())
ORDER BY agent_id NULLS LAST, priority_or_unknown NULLS LAST
