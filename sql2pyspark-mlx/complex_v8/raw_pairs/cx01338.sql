WITH base AS (
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
        tags,
        LENGTH(TRIM(channel)) AS channel_len,
        DATE_FORMAT(opened_at, 'yyyy-MM-dd HH:00') AS opened_at_label
    FROM tickets
    WHERE priority IN ('P1', 'P3', 'P4')
      AND requester_id > 7
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN t.actor = 'agent' THEN 1 END) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_actor_count,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at
    FROM base s
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        priority,
        channel,
        csat,
        tags,
        channel_len,
        event_actor_count,
        last_event_at,
        COUNT(*) OVER (PARTITION BY channel_len ORDER BY channel_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MIN(csat) OVER (PARTITION BY csat) AS csat_min_csat,
        MIN(requester_id) OVER w_csat AS csat_min_requester_id,
        MIN(channel_len) OVER (PARTITION BY channel_len ORDER BY channel_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_channel_len_so_far,
        RANK() OVER w_all_by_channel_len_ties AS channel_len_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY channel_len), 3) AS channel_len_percent_rank
    FROM profiled
    WINDOW w_csat AS (PARTITION BY csat), w_all_by_channel_len_ties AS (ORDER BY channel_len)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, metric_value
FROM (
    SELECT id, channel_len, csat_min_csat, min_channel_len_so_far, event_actor_count
    FROM sequenced
) src
UNPIVOT (
    metric_value FOR measure IN (channel_len AS channel_len_v, csat_min_csat AS csat_min_csat_v, min_channel_len_so_far AS m_min_channel_len_so_far, event_actor_count AS m_event_actor_count)
) unp
