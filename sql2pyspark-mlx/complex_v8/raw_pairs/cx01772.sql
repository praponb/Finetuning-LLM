WITH prepared_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE csat < 4
),
with_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.priority,
        s.channel,
        s.csat,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM prepared_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(csat) OVER (PARTITION BY agent_id) AS agent_id_max_csat,
        ROUND(AVG(requester_id) OVER (PARTITION BY csat, channel ORDER BY csat DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_3,
        NTILE(10) OVER (PARTITION BY priority ORDER BY csat, id) AS csat_decile,
        MIN(csat) OVER w_priority AS priority_min_csat,
        ROUND(PERCENT_RANK() OVER w_all_by_requester_id, 3) AS requester_id_percent_rank,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY priority), 0), 4) AS csat_share_of_priority,
        RANK() OVER (ORDER BY csat DESC NULLS FIRST, id) AS csat_rank,
        RANK() OVER (ORDER BY csat DESC NULLS FIRST) AS csat_rank_2,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY agent_id)) AS distinct_priority_in_agent_id,
        NTH_VALUE(csat, 2) OVER (PARTITION BY csat, channel ORDER BY csat DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat
    FROM with_details
    WINDOW w_priority AS (PARTITION BY priority), w_all_by_requester_id AS (ORDER BY requester_id DESC, id)
),
windowed_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        agent_id,
        agent_hired_on,
        requester_id_avg_3,
        csat_decile,
        priority_min_csat,
        csat_rank,
        csat_rank_2,
        distinct_priority_in_agent_id,
        second_csat,
        COUNT(*) OVER w_csat_rank_2_csat_decile AS rows_in_csat_rank_2_csat_decile,
        MIN(requester_id_avg_3) OVER (PARTITION BY csat_rank_2, csat_decile) AS csat_rank_2_csat_decile_min_requester_id_avg_3,
        MIN(requester_id_percent_rank) OVER (PARTITION BY csat_rank_2, csat_decile) AS csat_rank_2_csat_decile_min_requester_id_percent
    FROM windowed
    WINDOW w_csat_rank_2_csat_decile AS (PARTITION BY csat_rank_2, csat_decile)
)
SELECT
    id,
    requester_id,
    agent_id,
    agent_hired_on,
    requester_id_avg_3,
    priority_min_csat,
    csat_rank,
    csat_rank_2,
    distinct_priority_in_agent_id,
    second_csat,
    rows_in_csat_rank_2_csat_decile,
    csat_rank_2_csat_decile_min_requester_id_avg_3
FROM windowed_2
