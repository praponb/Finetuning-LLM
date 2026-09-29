WITH prepared AS (
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
        tags
    FROM tickets
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(requester_id) OVER (PARTITION BY csat) AS csat_max_requester_id,
        MIN(requester_id) OVER (ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_requester_id_so_far,
        COUNT(*) OVER (PARTITION BY csat) AS rows_in_csat,
        MAX(requester_id) OVER (ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_requester_id_so_far,
        ROUND(PERCENT_RANK() OVER (ORDER BY opened_at DESC, id), 3) AS opened_at_percent_rank,
        MIN(csat) OVER (PARTITION BY csat) AS csat_min_csat
    FROM prepared
),
with_details_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.channel,
        s.csat,
        s.csat_max_requester_id,
        s.min_requester_id_so_far,
        s.rows_in_csat,
        s.max_requester_id_so_far,
        s.csat_min_csat,
        su.agent_name
    FROM ranked s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE LENGTH(TRIM(su.agent_name)) >= 6
)
SELECT *
FROM (
    SELECT closed_at, opened_at, channel, csat_max_requester_id
    FROM with_details_agent
) src
PIVOT (
    AVG(csat_max_requester_id)
    FOR channel IN ('chat', 'email', 'web', 'phone')
)
ORDER BY closed_at, opened_at
