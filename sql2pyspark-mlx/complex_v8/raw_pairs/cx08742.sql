WITH recent_tickets AS (
    -- Scope the tickets rows
    SELECT *
    FROM tickets
    WHERE agent_id IS NOT NULL
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY channel, agent_id), 0), 4) AS requester_id_share_of_channel_agent_id,
        SUM(requester_id) OVER (PARTITION BY channel, agent_id) AS channel_agent_id_sum_requester_id,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY channel, agent_id), 0), 4) AS csat_share_of_channel_agent_id,
        ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY closed_at DESC, id) AS closed_at_seq,
        DENSE_RANK() OVER w_priority_by_csat_ties AS csat_dense_rank,
        FIRST_VALUE(closed_at) OVER (PARTITION BY TO_DATE(closed_at) ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_closed_at,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY priority ORDER BY csat DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        COUNT(*) OVER (PARTITION BY channel, agent_id) AS rows_in_channel_agent_id,
        LAST_VALUE(csat) OVER (PARTITION BY agent_id ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_csat,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_id ORDER BY closed_at DESC, id), 3) AS closed_at_percent_rank,
        FIRST_VALUE(requester_id) OVER (PARTITION BY priority ORDER BY csat DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_requester_id
    FROM recent_tickets
    WINDOW w_priority_by_csat_ties AS (PARTITION BY priority ORDER BY csat DESC NULLS FIRST)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE csat_dense_rank = 1
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        channel_agent_id_sum_requester_id,
        csat_share_of_channel_agent_id,
        csat_dense_rank,
        first_closed_at,
        second_requester_id,
        rows_in_channel_agent_id,
        last_csat,
        first_requester_id,
        COUNT(*) OVER (PARTITION BY priority, csat) AS rows_in_priority_csat,
        LAG(rows_in_channel_agent_id, 2) OVER w_last_csat_by_closed_at AS prev2_rows_in_channel_agent_id,
        ROUND(PERCENT_RANK() OVER w_last_csat_by_closed_at_ties, 3) AS closed_at_percent_rank_2,
        closed_at_seq - LAG(closed_at_seq) OVER (PARTITION BY csat ORDER BY csat_dense_rank, id) AS closed_at_seq_change,
        MAX(closed_at_seq) OVER (PARTITION BY priority, csat) AS priority_csat_max_closed_at_seq,
        FIRST_VALUE(requester_id) OVER (PARTITION BY last_csat ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_requester_id_2,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY priority, csat)) AS distinct_agent_id_in_priority_csat,
        csat - LAG(csat) OVER (PARTITION BY MONTH(closed_at), channel ORDER BY closed_at, id) AS csat_change
    FROM top_rows
    WINDOW w_last_csat_by_closed_at AS (PARTITION BY last_csat ORDER BY closed_at, id), w_last_csat_by_closed_at_ties AS (PARTITION BY last_csat ORDER BY closed_at)
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    priority,
    channel,
    csat,
    csat_share_of_channel_agent_id,
    csat_dense_rank,
    last_csat,
    first_requester_id,
    prev2_rows_in_channel_agent_id,
    closed_at_percent_rank_2,
    closed_at_seq_change,
    priority_csat_max_closed_at_seq,
    first_requester_id_2,
    distinct_agent_id_in_priority_csat,
    csat_change
FROM sequenced
WHERE last_csat BETWEEN 1 AND 4
ORDER BY closed_at_percent_rank_2 NULLS LAST
