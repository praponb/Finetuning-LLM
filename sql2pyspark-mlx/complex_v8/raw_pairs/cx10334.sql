WITH filtered AS (
    -- Keep valid rows and derive helper columns
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
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug
    FROM tickets
    WHERE csat BETWEEN 2 AND 5
      AND SIZE(tags) >= 1
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at
    FROM filtered s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count
    FROM profiled s
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN channel IN ('chat', 'email') THEN 'group_a' WHEN channel = 'web' THEN 'group_b' ELSE 'other' END AS channel_group,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown
    FROM with_counts
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        opened_at,
        closed_at,
        priority,
        channel,
        tags,
        channel_slug,
        channel_or_unknown,
        SUM(ticket_count) OVER (PARTITION BY channel_or_unknown ORDER BY requester_id, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS ticket_count_sum_4,
        COUNT(*) OVER (PARTITION BY channel, ticket_count) AS rows_in_channel_ticket_count
    FROM enriched
),
event_stats_2 AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(*) AS event_count,
        COUNT(*) AS event_count_2,
        COUNT(CASE WHEN event_type = 'resolved' THEN 1 END) AS event_event_type_count
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count,
        COALESCE(t.event_count_2, 0) AS event_count_2,
        COALESCE(t.event_event_type_count, 0) AS event_event_type_count
    FROM with_trends s
    LEFT JOIN event_stats_2 t ON t.ticket_id = s.id
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM summarized_rows s
    WHERE s.id IN (
            SELECT t.ticket_id
            FROM ticket_events t
        )
)
-- Final projection
SELECT
    id,
    closed_at,
    priority,
    channel,
    tags,
    channel_slug,
    channel_or_unknown,
    ticket_count_sum_4,
    event_count_2,
    event_event_type_count,
    DATE_TRUNC('QUARTER', closed_at) AS closed_at_quarter
FROM with_activity
ORDER BY channel_or_unknown NULLS LAST, channel
