WITH filtered_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        csat,
        tags,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 60, 1) AS minutes_opened_at_to_closed_at
    FROM tickets
    WHERE priority IN ('P2', 'P4', 'P3')
),
deduped AS (
    -- Keep the latest row per priority, agent_id
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        csat,
        tags,
        minutes_opened_at_to_closed_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY priority, agent_id ORDER BY requester_id DESC, id DESC) AS row_num
        FROM filtered_tickets
    ) ranked
    WHERE row_num = 1
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_count,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS first_event_at,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS first_event_at_2
    FROM deduped s
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        agent_id,
        priority,
        channel,
        csat,
        tags,
        minutes_opened_at_to_closed_at,
        event_count,
        SUM(csat) OVER (PARTITION BY agent_id, event_count) AS agent_id_event_count_sum_csat,
        ROUND(csat - AVG(csat) OVER w_agent_id_event_count, 2) AS csat_vs_agent_id_event_count_avg,
        COUNT(*) OVER (PARTITION BY agent_id, event_count) AS rows_in_agent_id_event_count,
        ROUND(AVG(csat) OVER (PARTITION BY channel ORDER BY first_event_at_2 DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS csat_avg_4
    FROM profiled
    WINDOW w_agent_id_event_count AS (PARTITION BY agent_id, event_count)
)
SELECT id, field, reading
FROM (
    SELECT id, agent_id_event_count_sum_csat, event_count
    FROM with_trends
) src
UNPIVOT (
    reading FOR field IN (agent_id_event_count_sum_csat, event_count)
) unp
ORDER BY id, field
