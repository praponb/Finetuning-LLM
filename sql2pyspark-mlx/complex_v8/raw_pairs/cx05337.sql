WITH recent AS (
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
    WHERE csat BETWEEN 1 AND 5
),
enriched AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        su.hired_on AS agent_hired_on,
        su.agent_name,
        su.tier AS agent_tier
    FROM recent s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE YEAR(su.hired_on) = 2024
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(requester_id - AVG(requester_id) OVER w_priority_opened_at_to_date, 2) AS requester_id_vs_priority_opened_at_to_date_avg,
        LAST_VALUE(closed_at) IGNORE NULLS OVER (PARTITION BY priority ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_closed_at,
        RANK() OVER (PARTITION BY priority, csat ORDER BY agent_hired_on DESC, id) AS agent_hired_on_rank,
        MAX(requester_id) OVER (PARTITION BY priority, TO_DATE(opened_at)) AS priority_opened_at_to_date_max_requester_id,
        ROUND(csat - AVG(csat) OVER (PARTITION BY priority, TO_DATE(opened_at)), 2) AS csat_vs_priority_opened_at_to_date_avg,
        LAG(agent_hired_on) OVER (PARTITION BY priority, csat ORDER BY agent_hired_on DESC, id) AS prev_agent_hired_on,
        ROW_NUMBER() OVER w_csat_by_closed_at AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY priority, csat ORDER BY agent_hired_on DESC, id) AS agent_hired_on_seq,
        ROW_NUMBER() OVER (PARTITION BY priority ORDER BY csat DESC, id) AS csat_seq,
        csat - LAG(csat) OVER (PARTITION BY priority, csat ORDER BY agent_hired_on DESC, id) AS csat_change,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY priority, TO_DATE(opened_at))) AS distinct_agent_id_in_priority_opened_at_to_date
    FROM enriched
    WINDOW w_priority_opened_at_to_date AS (PARTITION BY priority, TO_DATE(opened_at)), w_csat_by_closed_at AS (PARTITION BY csat ORDER BY closed_at, id)
)
-- Final projection
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
    agent_hired_on,
    agent_tier,
    requester_id_vs_priority_opened_at_to_date_avg,
    agent_hired_on_rank,
    priority_opened_at_to_date_max_requester_id,
    agent_hired_on_seq,
    csat_seq,
    distinct_agent_id_in_priority_opened_at_to_date
FROM with_windows
ORDER BY agent_tier, id
LIMIT 20
