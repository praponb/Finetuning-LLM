WITH cleaned_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE agent_id <= 8
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY csat, agent_id) AS rows_in_csat_agent_id,
        ROW_NUMBER() OVER w_channel_agent_id_by_requeste AS requester_id_seq,
        DENSE_RANK() OVER (ORDER BY csat DESC NULLS FIRST, id) AS csat_dense_rank,
        RANK() OVER (ORDER BY csat DESC NULLS FIRST, id) AS csat_rank,
        SUM(csat) OVER (PARTITION BY csat, agent_id) AS csat_agent_id_sum_csat,
        csat - LAG(csat) OVER w_channel_agent_id_by_requeste AS csat_change,
        DENSE_RANK() OVER (PARTITION BY channel, agent_id ORDER BY requester_id, id) AS requester_id_dense_rank,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY csat, agent_id), 0), 4) AS requester_id_share_of_csat_agent_id,
        ROUND(AVG(csat) OVER (PARTITION BY csat, agent_id), 2) AS csat_agent_id_avg_csat,
        ROUND(PERCENT_RANK() OVER (ORDER BY csat, id), 3) AS csat_percent_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY csat), 3) AS csat_percent_rank_2
    FROM cleaned_tickets
    WINDOW w_channel_agent_id_by_requeste AS (PARTITION BY channel, agent_id ORDER BY requester_id, id), w_csat_agent_id AS (PARTITION BY csat, agent_id)
),
summary AS (
    -- Group-level totals
    SELECT
        agent_id,
        MAX(csat_percent_rank) AS max_csat_percent_rank,
        COUNT(DISTINCT channel) AS distinct_channels
    FROM sequenced
    GROUP BY 1
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(distinct_channels - AVG(distinct_channels) OVER (PARTITION BY agent_id), 2) AS distinct_channels_vs_agent_id_avg,
        ROUND(AVG(distinct_channels) OVER (ORDER BY max_csat_percent_rank DESC, agent_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS distinct_channels_avg_7,
        NTH_VALUE(max_csat_percent_rank, 2) OVER (PARTITION BY distinct_channels ORDER BY max_csat_percent_rank ASC NULLS LAST, agent_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_max_csat_percent_rank,
        MAX(max_csat_percent_rank) OVER (PARTITION BY agent_id) AS agent_id_max_max_csat_percent_rank
    FROM summary
)
-- Final projection
SELECT
    agent_id,
    max_csat_percent_rank,
    distinct_channels,
    distinct_channels_vs_agent_id_avg,
    distinct_channels_avg_7,
    second_max_csat_percent_rank,
    ROUND(distinct_channels / NULLIF(distinct_channels_avg_7, 0), 3) AS distinct_channels_per_distinct_channels_avg_7
FROM windowed
WHERE second_max_csat_percent_rank >= 0.3
ORDER BY max_csat_percent_rank, distinct_channels_vs_agent_id_avg DESC, agent_id
LIMIT 10
