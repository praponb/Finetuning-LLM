WITH cleaned_tickets AS (
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
        CONCAT(channel, '#', CAST(requester_id AS STRING)) AS channel_ref
    FROM tickets
    WHERE priority IS NOT NULL
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER w_priority_by_requester_id AS requester_id_rank,
        LEAD(closed_at, 3) OVER (PARTITION BY agent_id, channel ORDER BY csat, id) AS next3_closed_at,
        csat - LAG(csat) OVER (PARTITION BY agent_id, channel ORDER BY csat, id) AS csat_change,
        LEAD(opened_at) OVER (PARTITION BY agent_id, channel ORDER BY csat, id) AS next_opened_at
    FROM cleaned_tickets
    WINDOW w_priority_by_requester_id AS (PARTITION BY priority ORDER BY requester_id, id)
),
flattened AS (
    -- One row per tag
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.channel_ref,
        s.requester_id_rank,
        s.next3_closed_at,
        s.csat_change,
        s.next_opened_at,
        t.tag
    FROM sequenced s
    LATERAL VIEW EXPLODE(s.tags) t AS tag
),
with_parent_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.csat,
        s.channel_ref,
        s.requester_id_rank,
        s.csat_change,
        s.next_opened_at,
        s.tag,
        su.agent_name
    FROM flattened s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE t.priority = 'P2') FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_priority_count,
        (SELECT MAX(t.csat) FROM tickets t WHERE t.agent_id = s.agent_id) AS max_ticket_csat
    FROM with_parent_agent s
),
with_activity AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM with_stats s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.actor = 'agent'
        )
),
summary AS (
    -- Summarise per group
    SELECT
        agent_id,
        ROUND(AVG(requester_id_rank), 2) AS avg_requester_id_rank,
        SUM(CASE WHEN LENGTH(TRIM(agent_name)) >= 8 THEN requester_id ELSE 0 END) AS requester_id_length_trim_agent_name_8,
        MIN(ticket_priority_count) AS min_ticket_priority_count,
        MIN(closed_at) AS first_closed_at,
        MAX(closed_at) AS last_closed_at,
        COUNT(*) AS row_count
    FROM with_activity
    GROUP BY agent_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT agent_id, measure, amount
FROM (
    SELECT agent_id, requester_id_length_trim_agent_name_8, row_count, min_ticket_priority_count
    FROM summary
) src
UNPIVOT (
    amount FOR measure IN (requester_id_length_trim_agent_name_8, row_count, min_ticket_priority_count)
) unp
