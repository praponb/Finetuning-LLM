WITH base_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        tags
    FROM tickets
    WHERE opened_at < TIMESTAMP '2025-02-24 08:00:00'
      AND agent_id IS NOT NULL
),
tagged AS (
    -- Label and score rows
    SELECT *, QUARTER(opened_at) AS opened_at_quarter
    FROM base_tickets
    WHERE channel = 'chat'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        channel,
        tags,
        opened_at_quarter,
        MAX(opened_at_quarter) OVER (PARTITION BY channel, TO_DATE(closed_at) ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_opened_at_quarter_so_far,
        FIRST_VALUE(opened_at_quarter) IGNORE NULLS OVER (PARTITION BY channel, TO_DATE(closed_at) ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_at_quarter,
        COUNT(*) OVER (ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        requester_id - LAG(requester_id) OVER w_opened_at_quarter_agent_id_b AS requester_id_change,
        MIN(requester_id) OVER w_priority_opened_at_quarter AS priority_opened_at_quarter_min_requester_id
    FROM tagged
    WINDOW w_opened_at_quarter_agent_id_b AS (PARTITION BY opened_at_quarter, agent_id ORDER BY closed_at DESC, id), w_priority_opened_at_quarter AS (PARTITION BY priority, opened_at_quarter)
)
SELECT
    id,
    requester_id,
    channel,
    tags,
    opened_at_quarter,
    max_opened_at_quarter_so_far,
    first_known_opened_at_quarter,
    requester_id_change,
    priority_opened_at_quarter_min_requester_id,
    running_count - requester_id AS running_count_minus_requester_id,
    COALESCE(requester_id, 0) AS requester_id_filled
FROM sequenced
ORDER BY id DESC, first_known_opened_at_quarter DESC NULLS FIRST
