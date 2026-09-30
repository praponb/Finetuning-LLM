WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat
    FROM tickets
    WHERE priority IS NOT NULL
),
enriched_agent AS (
    -- Join support_agents
    SELECT s.*, su.tier AS agent_tier, su.hired_on AS agent_hired_on
    FROM base s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        opened_at,
        priority,
        channel,
        agent_tier,
        agent_hired_on,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at)) AS rows_in_opened_at_month,
        FIRST_VALUE(csat) IGNORE NULLS OVER (PARTITION BY priority, channel ORDER BY csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_csat,
        FIRST_VALUE(agent_hired_on) IGNORE NULLS OVER (PARTITION BY priority, channel ORDER BY csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_agent_hired_on,
        ROW_NUMBER() OVER (ORDER BY csat DESC, id) AS rn,
        ROW_NUMBER() OVER w_priority_channel_by_csat AS rn_2,
        ROUND(csat - AVG(csat) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at)), 2) AS csat_vs_opened_at_month_avg,
        NTH_VALUE(agent_hired_on, 2) OVER (PARTITION BY priority, channel ORDER BY csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_agent_hired_on,
        LEAD(csat, 2) OVER (PARTITION BY priority, channel ORDER BY csat ASC, id) AS next2_csat,
        ROUND(AVG(csat) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at)), 2) AS opened_at_month_avg_csat,
        RANK() OVER (PARTITION BY agent_id ORDER BY csat) AS csat_rank,
        csat - LAG(csat) OVER w_all_by_csat AS csat_change
    FROM enriched_agent
    WINDOW w_priority_channel_by_csat AS (PARTITION BY priority, channel ORDER BY csat ASC, id), w_opened_at_month AS (PARTITION BY DATE_TRUNC('MONTH', opened_at)), w_all_by_csat AS (ORDER BY csat DESC, id)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        opened_at,
        priority,
        channel,
        agent_tier,
        agent_hired_on,
        rows_in_opened_at_month,
        first_known_csat,
        first_known_agent_hired_on,
        rn,
        rn_2,
        csat_vs_opened_at_month_avg,
        second_agent_hired_on,
        next2_csat,
        opened_at_month_avg_csat,
        csat_change
    FROM windowed
    WHERE csat_rank = 1
)
SELECT
    id,
    opened_at,
    priority,
    channel,
    agent_tier,
    agent_hired_on,
    rows_in_opened_at_month,
    first_known_csat,
    first_known_agent_hired_on,
    rn,
    rn_2,
    csat_vs_opened_at_month_avg,
    second_agent_hired_on,
    next2_csat,
    csat_change,
    CONCAT(agent_tier, '#', CAST(id AS STRING)) AS agent_tier_ref
FROM top_per_group
ORDER BY opened_at NULLS LAST, id
LIMIT 10
