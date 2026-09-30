WITH scoped AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        csat,
        tags,
        CONCAT(priority, '#', CAST(id AS STRING)) AS priority_ref,
        LENGTH(TRIM(channel)) AS channel_len
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'login')
      AND channel NOT IN ('email', 'phone')
),
enriched AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.csat,
        s.tags,
        s.channel_len,
        su.tier AS agent_tier,
        su.agent_name
    FROM scoped s
    JOIN support_agents su ON su.id = s.agent_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        LAG(csat) OVER (PARTITION BY agent_tier ORDER BY csat, id) AS prev_csat,
        channel_len - LAG(channel_len) OVER (PARTITION BY csat ORDER BY csat DESC, id) AS channel_len_change,
        NTH_VALUE(csat, 2) OVER (PARTITION BY csat ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat,
        channel_len - LAG(channel_len) OVER (PARTITION BY agent_tier ORDER BY csat, id) AS channel_len_change_2,
        ROUND(AVG(csat) OVER (PARTITION BY csat ORDER BY csat DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS csat_avg_3_prior,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY csat)) AS distinct_priority_in_csat,
        ROUND(csat - AVG(csat) OVER w_agent_id_csat, 2) AS csat_vs_agent_id_csat_avg,
        ROUND(PERCENT_RANK() OVER w_channel_len_priority_by_chan, 3) AS channel_len_percent_rank,
        LEAD(channel_len) OVER w_csat_by_csat AS next_channel_len
    FROM enriched
    WINDOW w_agent_id_csat AS (PARTITION BY agent_id, csat), w_channel_len_priority_by_chan AS (PARTITION BY channel_len, priority ORDER BY channel_len), w_csat_by_csat AS (PARTITION BY csat ORDER BY csat DESC, id)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(channel_len_percent_rank, 2) OVER (PARTITION BY csat, priority ORDER BY channel_len_percent_rank DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_channel_len_percent_rank,
        ROUND(channel_len_change_2 - AVG(channel_len_change_2) OVER w_channel_len, 2) AS channel_len_change_2_vs_channel_len_avg,
        LAST_VALUE(csat_vs_agent_id_csat_avg) OVER (ORDER BY prev_csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_csat_vs_agent_id_csat_avg
    FROM with_trends
    WINDOW w_channel_len AS (PARTITION BY channel_len)
)
SELECT
    id,
    agent_id,
    priority,
    tags,
    channel_len,
    agent_tier,
    agent_name,
    channel_len_change,
    channel_len_change_2,
    csat_avg_3_prior,
    distinct_priority_in_csat,
    csat_vs_agent_id_csat_avg,
    channel_len_percent_rank,
    next_channel_len,
    second_channel_len_percent_rank,
    channel_len_change_2_vs_channel_len_avg,
    last_csat_vs_agent_id_csat_avg
FROM metrics
ORDER BY channel_len_change_2_vs_channel_len_avg NULLS LAST
