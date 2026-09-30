WITH prepared AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        tags,
        CONCAT_WS(': ', channel, priority) AS channel_priority_label
    FROM tickets
    WHERE requester_id < 15
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        closed_at,
        tags,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY priority, agent_id ORDER BY closed_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        LAG(closed_at) OVER (PARTITION BY priority, agent_id ORDER BY closed_at ASC NULLS LAST, id) AS prev_closed_at,
        LEAD(closed_at) OVER w_priority_agent_id_by_closed_2 AS next_closed_at
    FROM prepared
    WINDOW w_priority_agent_id_by_closed_2 AS (PARTITION BY priority, agent_id ORDER BY closed_at ASC NULLS LAST, id)
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MAX(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS last_ticket_closed_at
    FROM with_trends s
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        agent_id,
        closed_at,
        tags,
        second_closed_at,
        prev_closed_at,
        next_closed_at,
        last_ticket_closed_at,
        DATE_FORMAT(next_closed_at, 'yyyy-MM-dd HH:00') AS next_closed_at_label,
        SIZE(tags) AS tags_count
    FROM summarized_rows
    WHERE MONTH(prev_closed_at) IN (2, 3)
)
-- Final projection
SELECT
    id,
    agent_id,
    closed_at,
    second_closed_at,
    next_closed_at,
    last_ticket_closed_at,
    next_closed_at_label,
    SUBSTRING(TRIM(next_closed_at_label), 1, 3) AS next_closed_at_label_prefix
FROM labeled
ORDER BY agent_id DESC, last_ticket_closed_at
