WITH base_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        CONCAT_WS(': ', priority, channel) AS priority_channel_label,
        COALESCE(requester_id, 0) AS requester_id_filled
    FROM tickets
    WHERE priority NOT IN ('P1', 'P4')
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(priority) OVER w_channel_agent_id) AS distinct_priority_in_channel_agent_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(opened_at), priority ORDER BY requester_id_filled NULLS LAST, id), 3) AS requester_id_filled_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY channel, priority ORDER BY requester_id, id) AS seq_no,
        ROUND(requester_id_filled - AVG(requester_id_filled) OVER (PARTITION BY agent_id), 2) AS requester_id_filled_vs_agent_id_avg,
        COUNT(*) OVER (PARTITION BY priority ORDER BY requester_id_filled ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY agent_id)) AS distinct_channel_in_agent_id,
        RANK() OVER (PARTITION BY priority ORDER BY requester_id_filled ASC) AS requester_id_filled_rank,
        SUM(requester_id) OVER (PARTITION BY channel, priority ORDER BY requester_id, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS requester_id_sum_3,
        NTILE(5) OVER w_channel_priority_by_requeste AS requester_id_quintile,
        requester_id - LAG(requester_id) OVER (PARTITION BY priority ORDER BY requester_id_filled ASC, id) AS requester_id_change,
        ROW_NUMBER() OVER (PARTITION BY MONTH(opened_at), priority ORDER BY requester_id_filled NULLS LAST, id) AS rn,
        SUM(requester_id_filled) OVER w_agent_id AS agent_id_sum_requester_id_filled
    FROM base_tickets
    WINDOW w_channel_agent_id AS (PARTITION BY channel, agent_id), w_channel_priority_by_requeste AS (PARTITION BY channel, priority ORDER BY requester_id, id), w_agent_id AS (PARTITION BY agent_id)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        channel,
        priority_channel_label,
        requester_id_filled,
        requester_id_filled_percent_rank,
        seq_no,
        running_count,
        distinct_channel_in_agent_id,
        requester_id_filled_rank,
        requester_id_sum_3,
        ROUND(AVG(requester_id) OVER (ORDER BY distinct_channel_in_agent_id ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_4,
        SUM(requester_id_sum_3) OVER (ORDER BY distinct_channel_in_agent_id ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id_sum_3,
        SUM(distinct_channel_in_agent_id) OVER w_requester_id_filled_rank AS requester_id_filled_rank_sum_distinct_channel_in,
        SIZE(COLLECT_SET(agent_id) OVER w_requester_id_filled_rank) AS distinct_agent_id_in_requester_id_filled_rank
    FROM with_windows
    WINDOW w_requester_id_filled_rank AS (PARTITION BY requester_id_filled_rank)
)
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    channel,
    priority_channel_label,
    requester_id_filled,
    requester_id_filled_percent_rank,
    seq_no,
    running_count,
    distinct_channel_in_agent_id,
    requester_id_filled_rank,
    running_requester_id_sum_3,
    requester_id_filled_rank_sum_distinct_channel_in,
    distinct_agent_id_in_requester_id_filled_rank,
    CASE WHEN channel IN ('phone', 'email') THEN 'group_a' WHEN channel = 'chat' THEN 'group_b' ELSE 'other' END AS channel_group,
    requester_id - requester_id_filled AS requester_id_minus_requester_id_filled
FROM sequenced
