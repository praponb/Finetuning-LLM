WITH base AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        tags
    FROM tickets
    WHERE channel <> 'web'
      AND priority <> 'P3'
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_count,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_count_2,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_count_3
    FROM base s
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER w_event_count_by_event_count_2 AS event_count_2_dense_rank,
        SUM(event_count_3) OVER (PARTITION BY agent_id) AS agent_id_sum_event_count_3,
        NTILE(10) OVER (PARTITION BY priority ORDER BY event_count_3, id) AS event_count_3_decile,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY agent_id), 2) AS requester_id_vs_agent_id_avg,
        ROUND(event_count - AVG(event_count) OVER (PARTITION BY agent_id), 2) AS event_count_vs_agent_id_avg,
        LEAD(event_count) OVER (PARTITION BY event_count ORDER BY event_count_2 DESC, id) AS next_event_count
    FROM with_stats
    WINDOW w_event_count_by_event_count_2 AS (PARTITION BY event_count ORDER BY event_count_2 DESC)
)
SELECT
    id,
    requester_id,
    tags,
    event_count_2,
    event_count_2_dense_rank,
    requester_id_vs_agent_id_avg,
    next_event_count,
    SUBSTRING(TRIM(priority), 1, 3) AS priority_prefix,
    GREATEST(event_count_2, event_count) AS greatest_event_count_2_event_count
FROM with_trends
ORDER BY greatest_event_count_2_event_count DESC, priority_prefix NULLS LAST
