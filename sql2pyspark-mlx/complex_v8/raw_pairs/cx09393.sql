WITH recent_tickets AS (
    -- Scope the tickets rows
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
        ABS(csat - 3) AS csat_distance,
        DATEDIFF(DATE '2025-12-31', opened_at) AS days_since_opened_at
    FROM tickets
    WHERE csat BETWEEN 1 AND 4
),
joined_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.csat,
        s.tags,
        s.csat_distance,
        su.hired_on AS agent_hired_on,
        su.agent_name,
        su.tier AS agent_tier
    FROM recent_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY agent_id, csat), 0), 4) AS csat_share_of_agent_id_csat,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on) ORDER BY agent_hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        MIN(csat_distance) OVER (PARTITION BY agent_id, DATE_TRUNC('MONTH', agent_hired_on)) AS agent_id_agent_hired_on_month_min_csat_distance,
        DENSE_RANK() OVER w_agent_hired_on_month_by_agen_2 AS agent_hired_on_dense_rank,
        LEAD(requester_id, 1, 0) OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on) ORDER BY agent_hired_on DESC, id) AS next_requester_id,
        csat - LAG(csat) OVER (PARTITION BY csat ORDER BY requester_id DESC, id) AS csat_change,
        LAG(csat_distance) OVER (PARTITION BY csat ORDER BY requester_id DESC, id) AS prev_csat_distance,
        LEAD(requester_id, 3) OVER w_agent_hired_on_year_by_csat AS next3_requester_id,
        RANK() OVER (PARTITION BY csat ORDER BY requester_id DESC, id) AS requester_id_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on) ORDER BY agent_hired_on DESC), 3) AS agent_hired_on_cume_dist,
        SUM(requester_id) OVER (PARTITION BY YEAR(agent_hired_on) ORDER BY csat_distance ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id,
        LAG(agent_hired_on) OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on) ORDER BY agent_hired_on DESC, id) AS prev_agent_hired_on,
        ROUND(AVG(requester_id) OVER (PARTITION BY agent_id, csat), 2) AS agent_id_csat_avg_requester_id,
        COUNT(*) OVER w_agent_id_csat AS rows_in_agent_id_csat,
        ROUND(PERCENT_RANK() OVER (PARTITION BY csat ORDER BY requester_id DESC, id), 3) AS requester_id_percent_rank
    FROM joined_agent
    WINDOW w_agent_hired_on_month_by_agen_2 AS (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on) ORDER BY agent_hired_on DESC), w_agent_hired_on_year_by_csat AS (PARTITION BY YEAR(agent_hired_on) ORDER BY csat_distance ASC, id), w_agent_id_csat AS (PARTITION BY agent_id, csat)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY agent_hired_on_cume_dist ASC, id) AS rn,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER (ORDER BY agent_hired_on_cume_dist ASC, id))) / 60, 1) AS minutes_since_prev_closed_at,
        ROUND(PERCENT_RANK() OVER w_prev_csat_distance_by_next3, 3) AS next3_requester_id_percent_rank
    FROM with_windows
    WINDOW w_prev_csat_distance_by_next3 AS (PARTITION BY prev_csat_distance ORDER BY next3_requester_id DESC, id)
)
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    csat,
    tags,
    csat_distance,
    agent_name,
    agent_tier,
    csat_share_of_agent_id_csat,
    second_requester_id,
    agent_id_agent_hired_on_month_min_csat_distance,
    agent_hired_on_dense_rank,
    csat_change,
    prev_csat_distance,
    next3_requester_id,
    agent_hired_on_cume_dist,
    running_requester_id,
    agent_id_csat_avg_requester_id,
    rows_in_agent_id_csat,
    rn,
    next3_requester_id_percent_rank
FROM windowed
ORDER BY prev_csat_distance, agent_name
