WITH cleaned_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags,
        DATE_TRUNC('DAY', closed_at) AS closed_at_day,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 60, 1) AS minutes_opened_at_to_closed_at
    FROM tickets
    WHERE MONTH(closed_at) IN (1, 3)
),
enriched_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.tags,
        s.closed_at_day,
        su.hired_on AS agent_hired_on,
        su.agent_name,
        su.team AS agent_team
    FROM cleaned_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(requester_id) OVER w_agent_id, 2) AS agent_id_avg_requester_id,
        MAX(requester_id) OVER (PARTITION BY priority, agent_id ORDER BY requester_id ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS requester_id_max_3_prior,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on)) AS rows_in_agent_hired_on_month,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on)), 0), 4) AS requester_id_share_of_agent_hired_on_month,
        SIZE(COLLECT_SET(agent_team) OVER (PARTITION BY agent_id)) AS distinct_agent_team_in_agent_id,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY agent_id)) AS distinct_priority_in_agent_id,
        ROUND(AVG(requester_id) OVER (PARTITION BY agent_id, DATE_TRUNC('MONTH', opened_at) ORDER BY closed_at_day, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_3,
        MAX(requester_id) OVER (PARTITION BY priority ORDER BY agent_hired_on DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS requester_id_max_3_prior_2,
        RANK() OVER (PARTITION BY agent_id ORDER BY requester_id DESC) AS requester_id_rank,
        DENSE_RANK() OVER (PARTITION BY agent_id ORDER BY requester_id DESC) AS requester_id_dense_rank,
        LAG(closed_at_day, 2) OVER w_agent_id_by_requester_id AS prev2_closed_at_day,
        ROUND(AVG(requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at_day), DATE_TRUNC('MONTH', opened_at) ORDER BY closed_at_day ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS requester_id_avg_2_prior,
        RANK() OVER (PARTITION BY priority, agent_id ORDER BY requester_id ASC) AS requester_id_rank_2
    FROM enriched_agent
    WINDOW w_agent_id AS (PARTITION BY agent_id), w_agent_id_by_requester_id AS (PARTITION BY agent_id ORDER BY requester_id DESC, id)
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        agent_id,
        opened_at,
        tags,
        closed_at_day,
        agent_name,
        requester_id_max_3_prior,
        rows_in_agent_hired_on_month,
        requester_id_share_of_agent_hired_on_month,
        distinct_agent_team_in_agent_id,
        requester_id_avg_3,
        requester_id_max_3_prior_2,
        requester_id_rank,
        requester_id_dense_rank,
        prev2_closed_at_day,
        requester_id_avg_2_prior,
        ARRAY_JOIN(ARRAY_SORT(tags), ',') AS tags_text
    FROM windowed
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    tags,
    closed_at_day,
    agent_name,
    requester_id_max_3_prior,
    requester_id_share_of_agent_hired_on_month,
    distinct_agent_team_in_agent_id,
    requester_id_avg_3,
    requester_id_max_3_prior_2,
    requester_id_rank,
    requester_id_dense_rank,
    prev2_closed_at_day,
    requester_id_avg_2_prior,
    tags_text
FROM enriched
WHERE requester_id_max_3_prior < 56
