WITH filtered_tickets AS (
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
    WHERE (requester_id >= 9 OR channel = 'email')
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.tags,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM filtered_tickets s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE UPPER(TRIM(su.agent_name)) LIKE 'L%'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY channel), 2) AS requester_id_vs_channel_avg,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER w_priority_agent_id, 0), 4) AS requester_id_share_of_priority_agent_id,
        NTILE(10) OVER (PARTITION BY priority, channel ORDER BY closed_at, id) AS closed_at_decile,
        COUNT(*) OVER w_agent_id_closed_at_to_date AS rows_in_agent_id_closed_at_to_date,
        NTH_VALUE(opened_at, 2) OVER (PARTITION BY agent_team ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at,
        ROUND(CUME_DIST() OVER w_agent_team_agent_id_by_agent, 3) AS agent_hired_on_cume_dist,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY TO_DATE(opened_at) ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        MIN(requester_id) OVER (PARTITION BY TO_DATE(opened_at) ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_requester_id_so_far,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER w_channel, 0), 4) AS requester_id_share_of_channel,
        COUNT(*) OVER (PARTITION BY priority, agent_id) AS rows_in_priority_agent_id,
        LEAD(requester_id, 1, 0) OVER (PARTITION BY TO_DATE(opened_at) ORDER BY opened_at DESC, id) AS next_requester_id,
        ROUND(AVG(requester_id) OVER (PARTITION BY agent_id, TO_DATE(closed_at)), 2) AS agent_id_closed_at_to_date_avg_requester_id
    FROM with_parent
    WINDOW w_priority_agent_id AS (PARTITION BY priority, agent_id), w_agent_id_closed_at_to_date AS (PARTITION BY agent_id, TO_DATE(closed_at)), w_agent_team_agent_id_by_agent AS (PARTITION BY agent_team, agent_id ORDER BY agent_hired_on), w_channel AS (PARTITION BY channel)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        opened_at,
        closed_at,
        priority,
        channel,
        agent_team,
        agent_hired_on,
        requester_id_vs_channel_avg,
        requester_id_share_of_priority_agent_id,
        rows_in_agent_id_closed_at_to_date,
        agent_hired_on_cume_dist,
        second_closed_at,
        requester_id_share_of_channel,
        rows_in_priority_agent_id,
        next_requester_id,
        agent_id_closed_at_to_date_avg_requester_id,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', second_opened_at) ORDER BY next_requester_id DESC, id), 3) AS next_requester_id_cume_dist,
        SIZE(COLLECT_SET(channel) OVER w_agent_hired_on_month) AS distinct_channel_in_agent_hired_on_month,
        LAST_VALUE(requester_id) OVER (PARTITION BY MONTH(opened_at), YEAR(agent_hired_on) ORDER BY agent_id_closed_at_to_date_avg_requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_requester_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(opened_at), YEAR(agent_hired_on) ORDER BY agent_id_closed_at_to_date_avg_requester_id DESC), 3) AS agent_id_closed_at_to_date_avg_requester_id_perc,
        ROUND(rows_in_priority_agent_id - AVG(rows_in_priority_agent_id) OVER (PARTITION BY agent_id, closed_at_decile), 2) AS rows_in_priority_agent_id_vs_agent_id_closed_at_,
        NTH_VALUE(second_opened_at, 2) OVER (ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_second_opened_at
    FROM with_windows
    WINDOW w_agent_hired_on_month AS (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on))
)
-- Final projection
SELECT
    id,
    opened_at,
    closed_at,
    priority,
    channel,
    agent_team,
    agent_hired_on,
    requester_id_vs_channel_avg,
    requester_id_share_of_priority_agent_id,
    rows_in_agent_id_closed_at_to_date,
    agent_hired_on_cume_dist,
    requester_id_share_of_channel,
    rows_in_priority_agent_id,
    next_requester_id,
    next_requester_id_cume_dist,
    distinct_channel_in_agent_hired_on_month,
    last_requester_id,
    rows_in_priority_agent_id_vs_agent_id_closed_at_,
    second_second_opened_at,
    LENGTH(TRIM(channel)) AS channel_len
FROM windowed
WHERE next_requester_id >= 8
ORDER BY requester_id_share_of_channel DESC, id
LIMIT 10
