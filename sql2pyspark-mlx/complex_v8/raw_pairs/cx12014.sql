WITH cleaned_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        tags
    FROM tickets
    WHERE opened_at < TIMESTAMP '2025-03-08 11:00:00'
      AND csat <= 4
),
joined_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        su.agent_name
    FROM cleaned_tickets s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE UPPER(TRIM(su.agent_name)) LIKE 'L%'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY closed_at ASC, id), 3) AS closed_at_percent_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY closed_at ASC), 3) AS closed_at_percent_rank_2,
        LAST_VALUE(closed_at) IGNORE NULLS OVER (ORDER BY closed_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_closed_at,
        LEAD(closed_at) OVER w_all_by_closed_at AS next_closed_at,
        LEAD(closed_at, 2) OVER (PARTITION BY priority ORDER BY closed_at ASC, id) AS next2_closed_at,
        ROW_NUMBER() OVER (PARTITION BY priority ORDER BY closed_at ASC, id) AS rn,
        NTILE(5) OVER (ORDER BY closed_at ASC, id) AS closed_at_quintile,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY priority ORDER BY closed_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        LAST_VALUE(closed_at) IGNORE NULLS OVER (PARTITION BY agent_id, priority ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_closed_at_2,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY agent_id, priority ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at_2,
        RANK() OVER (PARTITION BY priority ORDER BY closed_at ASC) AS closed_at_rank
    FROM joined_agent
    WINDOW w_all_by_closed_at AS (ORDER BY closed_at ASC, id)
),
first_rows AS (
    -- Top 3 per group
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        agent_name,
        closed_at_percent_rank,
        closed_at_percent_rank_2,
        last_known_closed_at,
        next_closed_at,
        next2_closed_at,
        closed_at_quintile,
        second_closed_at,
        last_known_closed_at_2,
        second_closed_at_2,
        closed_at_rank
    FROM windowed
    WHERE rn <= 3
)
SELECT
    id,
    agent_id,
    closed_at,
    agent_name,
    closed_at_percent_rank_2,
    last_known_closed_at,
    next_closed_at,
    closed_at_quintile,
    second_closed_at,
    last_known_closed_at_2,
    closed_at_rank
FROM first_rows
ORDER BY closed_at
