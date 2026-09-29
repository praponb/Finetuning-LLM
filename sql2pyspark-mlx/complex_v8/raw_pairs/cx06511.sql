WITH base_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        csat,
        tags,
        CASE WHEN priority IN ('P4', 'P2') THEN 'group_a' WHEN priority = 'P1' THEN 'group_b' ELSE 'other' END AS priority_group,
        DATE_TRUNC('QUARTER', opened_at) AS opened_at_quarter
    FROM tickets
    WHERE SIZE(tags) >= 2
),
joined_agent AS (
    -- Attach agent attributes
    SELECT s.*, su.hired_on AS agent_hired_on, su.agent_name
    FROM base_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        tags,
        agent_name,
        MIN(csat) OVER w_priority AS priority_min_csat,
        NTILE(10) OVER w_opened_at_month_opened_at_qu AS opened_at_decile,
        MAX(requester_id) OVER w_priority AS priority_max_requester_id,
        RANK() OVER w_opened_at_month_csat_by_requ AS requester_id_rank,
        COUNT(*) OVER (PARTITION BY priority) AS rows_in_priority,
        SUM(csat) OVER (PARTITION BY priority ORDER BY agent_hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat,
        SIZE(COLLECT_SET(priority_group) OVER (PARTITION BY priority)) AS distinct_priority_group_in_priority,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY priority), 0), 4) AS csat_share_of_priority,
        NTILE(4) OVER (PARTITION BY MONTH(opened_at), csat ORDER BY requester_id DESC, id) AS requester_id_quartile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY priority ORDER BY agent_hired_on, id), 3) AS agent_hired_on_percent_rank,
        ROUND(AVG(requester_id) OVER (PARTITION BY priority), 2) AS priority_avg_requester_id,
        ROUND(PERCENT_RANK() OVER w_opened_at_month_csat_by_requ, 3) AS requester_id_percent_rank,
        MIN(requester_id) OVER w_priority AS priority_min_requester_id,
        ROUND(CUME_DIST() OVER w_opened_at_month_csat_by_requ, 3) AS requester_id_cume_dist
    FROM joined_agent
    WINDOW w_priority AS (PARTITION BY priority), w_opened_at_month_opened_at_qu AS (PARTITION BY MONTH(opened_at), MONTH(opened_at_quarter) ORDER BY opened_at ASC NULLS LAST, id), w_opened_at_month_csat_by_requ AS (PARTITION BY MONTH(opened_at), csat ORDER BY requester_id DESC, id)
)
SELECT
    id,
    agent_id,
    opened_at,
    priority,
    tags,
    agent_name,
    priority_min_csat,
    opened_at_decile,
    priority_max_requester_id,
    requester_id_rank,
    rows_in_priority,
    distinct_priority_group_in_priority,
    requester_id_quartile,
    agent_hired_on_percent_rank,
    requester_id_percent_rank,
    requester_id_cume_dist,
    DAYOFWEEK(opened_at) AS opened_at_dayofweek
FROM metrics
ORDER BY agent_id
