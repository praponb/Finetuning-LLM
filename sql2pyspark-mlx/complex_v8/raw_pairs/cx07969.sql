WITH recent AS (
    -- Filter tickets
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
    WHERE agent_id IS NOT NULL
      AND channel <> 'phone'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(priority) OVER w_agent_id) AS distinct_priority_in_agent_id,
        DENSE_RANK() OVER (ORDER BY closed_at) AS closed_at_dense_rank,
        COUNT(*) OVER w_priority AS rows_in_priority,
        SUM(csat) OVER (PARTITION BY csat, agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat,
        LEAD(requester_id) OVER (PARTITION BY csat ORDER BY closed_at, id) AS next_requester_id,
        NTH_VALUE(opened_at, 2) OVER (ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at,
        SIZE(COLLECT_SET(agent_id) OVER w_priority) AS distinct_agent_id_in_priority,
        LEAD(opened_at) OVER (ORDER BY closed_at, id) AS next_opened_at,
        MAX(csat) OVER (PARTITION BY csat, agent_id ORDER BY opened_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS csat_max_4,
        MAX(csat) OVER (PARTITION BY priority ORDER BY csat DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS csat_max_3,
        MIN(requester_id) OVER (PARTITION BY agent_id) AS agent_id_min_requester_id
    FROM recent
    WINDOW w_agent_id AS (PARTITION BY agent_id), w_priority AS (PARTITION BY priority)
)
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
    distinct_priority_in_agent_id,
    closed_at_dense_rank,
    rows_in_priority,
    running_csat,
    second_opened_at,
    next_opened_at,
    agent_id_min_requester_id
FROM with_trends
