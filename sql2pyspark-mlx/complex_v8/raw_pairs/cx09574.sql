WITH scoped AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        tags,
        DATE_FORMAT(closed_at, 'yyyy-MM-dd') AS closed_at_label,
        ROUND(requester_id / NULLIF(csat, 0), 3) AS requester_id_per_csat
    FROM tickets
    WHERE priority IN ('P4', 'P1', 'P2')
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        agent_id,
        COUNT(*) AS row_count,
        ARRAY_SORT(COLLECT_SET(priority)) AS priority_list,
        COUNT(DISTINCT priority) AS distinct_prioritys,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids
    FROM scoped
    GROUP BY 1
    HAVING SUM(requester_id_per_csat) > 2.5
),
per_label AS (
    -- One row per priority_list_item
    SELECT
        s.agent_id,
        s.row_count,
        s.distinct_prioritys,
        s.distinct_agent_ids,
        t.priority_list_item_pos,
        t.priority_list_item
    FROM stats s
    LATERAL VIEW POSEXPLODE(s.priority_list) t AS priority_list_item_pos, priority_list_item
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        MIN(closed_at) AS first_ticket_closed_at,
        COUNT(*) AS ticket_count,
        MAX(opened_at) AS last_ticket_opened_at
    FROM tickets
    GROUP BY agent_id
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.first_ticket_closed_at,
        COALESCE(t.ticket_count, 0) AS ticket_count,
        t.last_ticket_opened_at
    FROM per_label s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
)
SELECT
    agent_id,
    row_count,
    distinct_agent_ids,
    priority_list_item_pos,
    priority_list_item,
    first_ticket_closed_at,
    ticket_count,
    last_ticket_opened_at
FROM summarized_rows
ORDER BY last_ticket_opened_at DESC, agent_id
