WITH valid AS (
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
        tags
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'login')
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count_2,
        (SELECT MAX(t.requester_id) FROM tickets t WHERE t.agent_id = s.agent_id) AS max_ticket_requester_id
    FROM valid s
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(ticket_count_2) OVER (ORDER BY ticket_count ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ticket_count_2,
        SUM(requester_id) OVER (PARTITION BY agent_id) AS agent_id_sum_requester_id,
        LAG(ticket_count_2, 1, 0) OVER w_all_by_ticket_count AS prev_ticket_count_2,
        ROUND(PERCENT_RANK() OVER (ORDER BY ticket_count ASC, id), 3) AS ticket_count_percent_rank
    FROM with_counts
    WINDOW w_all_by_ticket_count AS (ORDER BY ticket_count ASC, id)
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    channel,
    csat,
    tags,
    max_ticket_requester_id,
    last_ticket_count_2,
    agent_id_sum_requester_id,
    ticket_count_percent_rank,
    DATE_TRUNC('HOUR', opened_at) AS opened_at_hour
FROM metrics
ORDER BY channel, id
LIMIT 20
