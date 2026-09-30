WITH recent_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        tags,
        INITCAP(TRIM(priority)) AS priority_clean
    FROM tickets
    WHERE requester_id <= 18
),
combined_details AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.channel,
        s.priority_clean,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM recent_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER w_all_by_agent_hired_on AS agent_hired_on_quartile,
        ROUND(CUME_DIST() OVER (ORDER BY agent_hired_on DESC), 3) AS agent_hired_on_cume_dist,
        DATEDIFF(agent_hired_on, LAG(agent_hired_on) OVER (PARTITION BY TO_DATE(opened_at) ORDER BY opened_at ASC NULLS LAST, id)) AS days_since_prev_agent_hired_on,
        NTILE(5) OVER w_opened_at_to_date_by_opened AS opened_at_quintile,
        DENSE_RANK() OVER (PARTITION BY agent_id ORDER BY opened_at) AS opened_at_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_id ORDER BY opened_at, id), 3) AS opened_at_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY TO_DATE(opened_at) ORDER BY opened_at ASC NULLS LAST, id), 3) AS opened_at_percent_rank_2,
        RANK() OVER (PARTITION BY agent_id ORDER BY opened_at) AS opened_at_rank,
        ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS rn,
        FIRST_VALUE(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_opened_at
    FROM combined_details
    WINDOW w_all_by_agent_hired_on AS (ORDER BY agent_hired_on DESC, id), w_opened_at_to_date_by_opened AS (PARTITION BY TO_DATE(opened_at) ORDER BY opened_at ASC NULLS LAST, id)
)
SELECT
    id,
    agent_id,
    opened_at,
    channel,
    agent_hired_on,
    agent_tier,
    days_since_prev_agent_hired_on,
    opened_at_quintile,
    opened_at_percent_rank,
    opened_at_percent_rank_2,
    opened_at_rank,
    rn,
    COALESCE(opened_at_dense_rank, 0) AS opened_at_dense_rank_filled
FROM with_windows
ORDER BY agent_id, opened_at_percent_rank DESC NULLS FIRST
