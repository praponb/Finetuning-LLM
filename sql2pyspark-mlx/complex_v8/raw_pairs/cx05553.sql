WITH recent_tickets AS (
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
        LENGTH(TRIM(priority)) AS priority_len
    FROM tickets
    WHERE MONTH(opened_at) IN (1, 3)
      AND priority IS NOT NULL
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(csat - AVG(csat) OVER w_priority, 2) AS csat_vs_priority_avg,
        DENSE_RANK() OVER (ORDER BY opened_at) AS opened_at_dense_rank,
        SUM(priority_len) OVER w_priority AS priority_sum_priority_len
    FROM recent_tickets
    WINDOW w_priority AS (PARTITION BY priority)
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS first_event_at,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at
    FROM windowed s
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(opened_at_dense_rank / 2) * 2 AS opened_at_dense_rank_bucket,
        ABS(csat - 3) AS csat_distance
    FROM summarized_rows
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        priority_len,
        priority_sum_priority_len,
        first_event_at,
        last_event_at,
        opened_at_dense_rank_bucket - LAG(opened_at_dense_rank_bucket) OVER (PARTITION BY channel, csat ORDER BY opened_at_dense_rank_bucket DESC, id) AS opened_at_dense_rank_bucket_change,
        LAST_VALUE(priority_len) OVER (PARTITION BY channel, csat ORDER BY opened_at_dense_rank_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_priority_len,
        ROUND(csat_distance - AVG(csat_distance) OVER (PARTITION BY agent_id, priority), 2) AS csat_distance_vs_agent_id_priority_avg
    FROM scored
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    closed_at,
    priority,
    priority_len,
    priority_sum_priority_len,
    first_event_at,
    opened_at_dense_rank_bucket_change,
    last_priority_len
FROM metrics
