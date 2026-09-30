WITH scoped AS (
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
        tags,
        LEAST(csat, requester_id) AS least_csat_requester_id,
        LENGTH(TRIM(channel)) AS channel_len,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 60, 1) AS minutes_opened_at_to_closed_at
    FROM tickets
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(least_csat_requester_id) OVER (PARTITION BY MONTH(opened_at) ORDER BY least_csat_requester_id ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS least_csat_requester_id_sum_3_prior,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY TO_DATE(opened_at), agent_id)) AS distinct_priority_in_opened_at_to_date_agent_id,
        COUNT(*) OVER (PARTITION BY TO_DATE(opened_at), agent_id) AS rows_in_opened_at_to_date_agent_id,
        LAG(closed_at, 2) OVER (PARTITION BY priority, channel ORDER BY csat, id) AS prev2_closed_at
    FROM scoped
),
screened AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND su.tier IS NOT NULL
        )
)
SELECT id, metric, metric_value
FROM (
    SELECT id, csat, distinct_priority_in_opened_at_to_date_agent_id
    FROM screened
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric IN (csat AS m_csat, distinct_priority_in_opened_at_to_date_agent_id AS m_distinct_priority_in_opened_at_to_date_agent_i)
) unp
