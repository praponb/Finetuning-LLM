WITH valid AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        tags,
        DATEDIFF(DATE '2026-01-01', opened_at) AS days_since_opened_at,
        QUARTER(closed_at) AS closed_at_quarter
    FROM tickets
    WHERE MONTH(opened_at) IN (1, 3)
      AND ARRAY_CONTAINS(tags, 'upgrade')
),
flattened AS (
    -- One row per tag
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.priority,
        s.channel,
        s.days_since_opened_at,
        s.closed_at_quarter,
        t.tag
    FROM valid s
    LATERAL VIEW OUTER EXPLODE(s.tags) t AS tag
),
qualified AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM flattened s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.actor = 'customer'
              AND t.event_type = 'assigned'
        )
      AND s.days_since_opened_at < 595
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        COUNT(*) AS ticket_count,
        COUNT(*) AS ticket_count_2,
        MAX(requester_id) AS max_ticket_requester_id
    FROM tickets
    GROUP BY agent_id
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count,
        COALESCE(t.ticket_count_2, 0) AS ticket_count_2,
        t.max_ticket_requester_id
    FROM qualified s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        SUBSTRING(TRIM(channel), 1, 2) AS channel_prefix,
        ticket_count + days_since_opened_at AS ticket_count_plus_days_since_opened_at,
        CASE WHEN tag IN ('login', 'upgrade') THEN 'group_a' WHEN tag = 'outage' THEN 'group_b' ELSE 'other' END AS tag_group
    FROM with_counts
),
tagged_2 AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS('-', tag, channel) AS tag_channel_label,
        ROUND(ticket_count_2 / NULLIF(days_since_opened_at, 0), 3) AS ticket_count_2_per_days_since_opened_at,
        SUBSTRING(TRIM(priority), 1, 2) AS priority_prefix
    FROM tagged
)
SELECT id, tag, metric, amount
FROM (
    SELECT id, tag, ticket_count_plus_days_since_opened_at, closed_at_quarter, ticket_count_2
    FROM tagged_2
) src
UNPIVOT (
    amount FOR metric IN (ticket_count_plus_days_since_opened_at AS ticket_count_plus_days_since_opened_at_v, closed_at_quarter AS m_closed_at_quarter, ticket_count_2 AS ticket_count_2_v)
) unp
