WITH cleaned_tickets AS (
    -- Filter tickets
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
    WHERE requester_id <= 18
      AND agent_id <= 3
),
joined_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.channel,
        s.csat,
        s.tags,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM cleaned_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        DATEDIFF(agent_hired_on, LAG(agent_hired_on) OVER w_csat_agent_id_by_csat) AS days_since_prev_agent_hired_on,
        ROUND(AVG(csat) OVER (PARTITION BY csat), 2) AS csat_avg_csat,
        MIN(requester_id) OVER (PARTITION BY channel, csat) AS channel_csat_min_requester_id,
        RANK() OVER (PARTITION BY channel ORDER BY csat) AS csat_rank,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY csat)) AS distinct_channel_in_csat,
        COUNT(*) OVER (PARTITION BY csat) AS rows_in_csat,
        SUM(requester_id) OVER (PARTITION BY csat) AS csat_sum_requester_id,
        ROUND(AVG(csat) OVER (PARTITION BY channel, agent_team ORDER BY requester_id, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS csat_avg_3_prior,
        LEAD(csat) OVER w_channel_by_csat AS next_csat,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY channel, csat), 0), 4) AS requester_id_share_of_channel_csat,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY channel, csat)) AS distinct_agent_id_in_channel_csat,
        ROUND(PERCENT_RANK() OVER (PARTITION BY csat, MONTH(agent_hired_on) ORDER BY agent_hired_on DESC), 3) AS agent_hired_on_percent_rank
    FROM joined_agent
    WINDOW w_csat_agent_id_by_csat AS (PARTITION BY csat, agent_id ORDER BY csat DESC, id), w_channel_by_csat AS (PARTITION BY channel ORDER BY csat, id)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(csat_rank - AVG(csat_rank) OVER (PARTITION BY agent_id, agent_team), 2) AS csat_rank_vs_agent_id_agent_team_avg,
        ROUND(AVG(csat_sum_requester_id) OVER (ORDER BY distinct_channel_in_csat DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS csat_sum_requester_id_avg_3,
        ROUND(AVG(agent_hired_on_percent_rank) OVER (PARTITION BY agent_id, agent_team), 2) AS agent_id_agent_team_avg_agent_hired_on_percent_r,
        MAX(next_csat) OVER (ORDER BY distinct_channel_in_csat, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS next_csat_max_4
    FROM windowed
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        next_csat,
        DATE_FORMAT(agent_hired_on, 'yyyy-MM') AS agent_hired_on_ym,
        channel,
        ARRAY_SORT(COLLECT_SET(channel)) AS channel_list,
        COUNT(*) AS row_count
    FROM with_trends
    GROUP BY 1, 2, 3
)
SELECT
    next_csat,
    agent_hired_on_ym,
    channel,
    channel_list
FROM stats
WHERE SIZE(channel_list) >= 1
ORDER BY channel, next_csat DESC
