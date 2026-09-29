WITH recent AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        csat,
        tags,
        DATEDIFF(DATE '2025-12-31', opened_at) AS days_since_opened_at
    FROM tickets
    WHERE HOUR(opened_at) BETWEEN 6 AND 11
),
with_details_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.priority,
        s.csat,
        s.tags,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM recent s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(agent_hired_on) IGNORE NULLS OVER (ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_agent_hired_on,
        LAST_VALUE(requester_id) OVER (ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_requester_id,
        SUM(csat) OVER (ORDER BY csat, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS csat_sum_3,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY csat, MONTH(agent_hired_on))) AS distinct_agent_id_in_csat_agent_hired_on_month
    FROM with_details_agent
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN csat_sum_3 >= 10 THEN 'A' WHEN csat_sum_3 >= 4 THEN 'B' ELSE 'C' END AS csat_sum_3_tier,
        CONCAT(priority, '#', CAST(last_requester_id AS STRING)) AS priority_ref,
        ADD_MONTHS(agent_hired_on, 12) AS agent_hired_on_plus_12m
    FROM windowed
)
SELECT
    id,
    agent_id,
    priority,
    agent_hired_on,
    agent_tier,
    agent_team,
    last_known_agent_hired_on,
    last_requester_id,
    csat_sum_3,
    distinct_agent_id_in_csat_agent_hired_on_month,
    csat_sum_3_tier,
    priority_ref,
    agent_hired_on_plus_12m,
    SIZE(tags) AS tags_count,
    CASE WHEN csat_sum_3 >= 9 THEN 'HIGH' WHEN csat_sum_3 >= 4 THEN 'MEDIUM' ELSE 'LOW' END AS csat_sum_3_tier_2
FROM tagged
ORDER BY last_requester_id DESC
