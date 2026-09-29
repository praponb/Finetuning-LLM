WITH valid AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        GREATEST(csat, requester_id) AS greatest_csat_requester_id,
        GREATEST(csat, requester_id) AS greatest_csat_requester_id_2
    FROM tickets
    WHERE agent_id <= 6
      AND priority NOT IN ('P1', 'P2')
),
latest AS (
    -- Keep the latest row per priority, agent_id
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        greatest_csat_requester_id,
        greatest_csat_requester_id_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY priority, agent_id ORDER BY closed_at DESC, id DESC) AS recency_rank
        FROM valid
    ) ranked
    WHERE recency_rank = 1
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, COUNT(*) AS ticket_count
    FROM tickets
    GROUP BY agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count
    FROM latest s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_stats
    WHERE greatest_csat_requester_id_2 > (SELECT AVG(greatest_csat_requester_id_2) FROM with_stats)
),
ticket_stats_2 AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        COUNT(CASE WHEN priority IN ('P3', 'P4') THEN 1 END) AS ticket_priority_count,
        COUNT(CASE WHEN priority IS NOT NULL THEN 1 END) AS ticket_priority_count_2
    FROM tickets
    GROUP BY agent_id
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_priority_count, 0) AS ticket_priority_count,
        COALESCE(t.ticket_priority_count_2, 0) AS ticket_priority_count_2
    FROM qualified s
    LEFT JOIN ticket_stats_2 t ON t.agent_id = s.agent_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY ticket_count, agent_id) AS rows_in_ticket_count_agent_id,
        NTILE(5) OVER w_priority_ticket_priority_cou AS greatest_csat_requester_id_quintile,
        ROUND(AVG(ticket_priority_count) OVER (PARTITION BY ticket_count, agent_id), 2) AS ticket_count_agent_id_avg_ticket_priority_count,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY ticket_count, agent_id)) AS distinct_channel_in_ticket_count_agent_id
    FROM with_counts
    WINDOW w_priority_ticket_priority_cou AS (PARTITION BY priority, ticket_priority_count ORDER BY greatest_csat_requester_id, id)
)
SELECT id, field, value
FROM (
    SELECT id, csat, ticket_priority_count, rows_in_ticket_count_agent_id, ticket_count
    FROM metrics
) src
UNPIVOT (
    value FOR field IN (csat, ticket_priority_count, rows_in_ticket_count_agent_id, ticket_count)
) unp
ORDER BY id, field
