WITH prepared AS (
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
        DATE_TRUNC('QUARTER', closed_at) AS closed_at_quarter,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 60, 1) AS minutes_opened_at_to_closed_at
    FROM tickets
    WHERE csat > 1
      AND requester_id < 18
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_count,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at
    FROM prepared s
),
latest_per_key AS (
    -- Keep the latest row per agent_id, priority
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
        closed_at_quarter,
        channel_or_unknown,
        minutes_opened_at_to_closed_at,
        event_count,
        last_event_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id, priority ORDER BY last_event_at DESC, id DESC) AS row_num
        FROM with_counts
    ) ranked
    WHERE row_num = 1
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(csat - AVG(csat) OVER w_event_count_csat, 2) AS csat_vs_event_count_csat_avg,
        SIZE(COLLECT_SET(priority) OVER w_event_count_csat) AS distinct_priority_in_event_count_csat,
        MAX(csat) OVER (PARTITION BY event_count, csat) AS event_count_csat_max_csat
    FROM latest_per_key
    WINDOW w_event_count_csat AS (PARTITION BY event_count, csat)
)
-- Subtotals with ROLLUP
SELECT
    channel_or_unknown,
    event_count,
    channel,
    MIN(distinct_priority_in_event_count_csat) AS min_distinct_priority_in_event_count_csat
FROM windowed
GROUP BY ROLLUP(channel_or_unknown, event_count, channel)
ORDER BY channel_or_unknown NULLS LAST, event_count NULLS LAST, channel NULLS LAST
