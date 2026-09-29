WITH cleaned_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        tags
    FROM tickets
    WHERE requester_id < 22
),
combined_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.tags,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team,
        su.tier AS agent_tier
    FROM cleaned_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY agent_id, agent_tier ORDER BY agent_hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        RANK() OVER (PARTITION BY agent_id, agent_tier ORDER BY agent_hired_on DESC, id) AS agent_hired_on_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_id, agent_tier ORDER BY agent_hired_on DESC, id), 3) AS agent_hired_on_cume_dist,
        NTH_VALUE(agent_hired_on, 2) OVER (PARTITION BY priority ORDER BY closed_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_agent_hired_on,
        LAG(agent_hired_on) OVER (PARTITION BY priority, agent_id ORDER BY agent_hired_on DESC, id) AS prev_agent_hired_on,
        RANK() OVER (PARTITION BY agent_id ORDER BY agent_hired_on ASC, id) AS agent_hired_on_rank_2,
        ROW_NUMBER() OVER w_agent_id_agent_tier_by_agent_2 AS agent_hired_on_seq,
        DENSE_RANK() OVER w_agent_id_by_agent_hired_on AS agent_hired_on_dense_rank,
        LAG(closed_at, 3) OVER w_agent_hired_on_month_by_clos AS prev3_closed_at,
        NTILE(5) OVER w_all_by_closed_at AS closed_at_quintile,
        FIRST_VALUE(agent_hired_on) IGNORE NULLS OVER (PARTITION BY priority, agent_id ORDER BY agent_hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_agent_hired_on,
        RANK() OVER (PARTITION BY priority, agent_id ORDER BY agent_hired_on DESC) AS agent_hired_on_rank_3
    FROM combined_details_agent
    WINDOW w_agent_id_agent_tier_by_agent_2 AS (PARTITION BY agent_id, agent_tier ORDER BY agent_hired_on DESC, id), w_agent_id_by_agent_hired_on AS (PARTITION BY agent_id ORDER BY agent_hired_on ASC, id), w_agent_hired_on_month_by_clos AS (PARTITION BY MONTH(agent_hired_on) ORDER BY closed_at ASC, id), w_all_by_closed_at AS (ORDER BY closed_at DESC, id)
),
leaders AS (
    -- Top 2 per group
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        tags,
        agent_hired_on,
        agent_team,
        agent_tier,
        second_closed_at,
        agent_hired_on_rank,
        agent_hired_on_cume_dist,
        second_agent_hired_on,
        prev_agent_hired_on,
        agent_hired_on_rank_2,
        agent_hired_on_seq,
        agent_hired_on_dense_rank,
        prev3_closed_at,
        closed_at_quintile,
        first_known_agent_hired_on
    FROM windowed
    WHERE agent_hired_on_rank_3 <= 2
)
SELECT
    id,
    closed_at,
    priority,
    agent_team,
    agent_tier,
    second_closed_at,
    agent_hired_on_rank,
    agent_hired_on_cume_dist,
    second_agent_hired_on,
    prev_agent_hired_on,
    agent_hired_on_rank_2,
    prev3_closed_at
FROM leaders
WHERE agent_hired_on_rank_2 >= 3
ORDER BY agent_hired_on_rank_2 DESC
