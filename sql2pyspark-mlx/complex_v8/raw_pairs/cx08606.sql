WITH prepared_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        csat,
        GREATEST(csat, requester_id) AS greatest_csat_requester_id,
        COALESCE(requester_id, 0) AS requester_id_filled,
        CASE WHEN csat >= 5 THEN 'HIGH' WHEN csat >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS csat_tier
    FROM tickets
    WHERE requester_id <= 14
      AND csat > 3
),
csat_tier_stats AS (
    -- Max requester_id_filled per csat_tier
    SELECT csat_tier, MAX(requester_id_filled) AS max_requester_id_filled
    FROM prepared_tickets
    GROUP BY csat_tier
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_requester_id_filled,
        ROUND(s.requester_id_filled - g.max_requester_id_filled, 2) AS requester_id_filled_minus_max
    FROM prepared_tickets s
    JOIN csat_tier_stats g ON g.csat_tier = s.csat_tier
    WHERE s.requester_id_filled = g.max_requester_id_filled
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        csat,
        csat_tier,
        max_requester_id_filled,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at) ORDER BY requester_id_filled ASC, id))) / 60, 1) AS minutes_since_prev_opened_at,
        ROUND(AVG(greatest_csat_requester_id) OVER (PARTITION BY csat_tier, csat), 2) AS csat_tier_csat_avg_greatest_csat_requester_id,
        NTILE(3) OVER (PARTITION BY csat_tier, agent_id ORDER BY requester_id_filled, id) AS requester_id_filled_tercile
    FROM above_average
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MAX(t.csat) FROM tickets t WHERE t.agent_id = s.agent_id) AS max_ticket_csat,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count,
        (SELECT MAX(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS last_ticket_closed_at
    FROM ranked s
)
-- Final projection
SELECT
    id,
    agent_id,
    csat_tier,
    max_requester_id_filled,
    minutes_since_prev_opened_at,
    requester_id_filled_tercile,
    ticket_count,
    last_ticket_closed_at,
    COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
FROM with_counts
WHERE csat_tier IN ('HIGH', 'LOW')
ORDER BY last_ticket_closed_at, max_requester_id_filled DESC
