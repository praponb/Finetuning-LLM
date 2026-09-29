WITH filtered_tickets AS (
    -- Keep valid rows and derive helper columns
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
        TO_DATE(closed_at) AS closed_at_date
    FROM tickets
    WHERE channel IS NOT NULL
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        tags,
        ROW_NUMBER() OVER w_csat_agent_id_by_requester_i AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY closed_at ASC, id) AS seq_no_2,
        RANK() OVER (PARTITION BY csat, agent_id ORDER BY requester_id NULLS LAST) AS requester_id_rank,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY csat, priority)) AS distinct_agent_id_in_csat_priority,
        ROUND(AVG(requester_id) OVER (PARTITION BY csat, agent_id ORDER BY csat DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_4
    FROM filtered_tickets
    WINDOW w_csat_agent_id_by_requester_i AS (PARTITION BY csat, agent_id ORDER BY requester_id NULLS LAST, id)
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        su.agent_name,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM ranked s
    JOIN support_agents su ON su.id = s.agent_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at
    FROM enriched s
)
SELECT id, metric_name, reading
FROM (
    SELECT id, seq_no_2, requester_id_rank, distinct_agent_id_in_csat_priority
    FROM summarized_rows
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric_name IN (seq_no_2 AS seq_no_2_v, requester_id_rank AS requester_id_rank_v, distinct_agent_id_in_csat_priority AS m_distinct_agent_id_in_csat_priority)
) unp
