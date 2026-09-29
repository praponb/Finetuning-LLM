WITH filtered_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        csat,
        tags,
        DATE_FORMAT(closed_at, 'yyyy-MM') AS closed_at_label,
        CONCAT(priority, '#', CAST(csat AS STRING)) AS priority_ref
    FROM tickets
    WHERE HOUR(opened_at) BETWEEN 10 AND 12
      AND channel IN ('phone', 'chat', 'email')
),
screened AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM filtered_tickets s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.actor = 'customer'
              AND TO_DATE(t.event_at) < DATE '2025-03-08'
        )
),
combined_details AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.csat,
        s.tags,
        s.closed_at_label,
        s.priority_ref,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM screened s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.tier = 'T1'
),
exploded AS (
    -- One row per tag
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.csat,
        s.closed_at_label,
        s.priority_ref,
        s.agent_tier,
        s.agent_team,
        t.tag
    FROM combined_details s
    LATERAL VIEW EXPLODE(s.tags) t AS tag
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        MIN(csat) AS min_ticket_csat,
        COUNT(*) FILTER (WHERE channel IN ('email', 'web')) AS ticket_channel_count
    FROM tickets
    GROUP BY agent_id
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.min_ticket_csat,
        COALESCE(t.ticket_channel_count, 0) AS ticket_channel_count
    FROM exploded s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        closed_at,
        priority,
        csat,
        closed_at_label,
        priority_ref,
        agent_tier,
        tag,
        ticket_channel_count,
        DENSE_RANK() OVER (PARTITION BY agent_tier, priority ORDER BY ticket_channel_count DESC NULLS FIRST) AS ticket_channel_count_dense_rank,
        SIZE(COLLECT_SET(agent_tier) OVER (PARTITION BY priority)) AS distinct_agent_tier_in_priority,
        SUM(ticket_channel_count) OVER (PARTITION BY agent_id, ticket_channel_count ORDER BY closed_at DESC NULLS FIRST, id, tag ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS ticket_channel_count_sum_1_prior,
        ROUND(ticket_channel_count / NULLIF(SUM(ticket_channel_count) OVER (PARTITION BY priority), 0), 4) AS ticket_channel_count_share_of_priority,
        NTH_VALUE(csat, 2) OVER (ORDER BY ticket_channel_count, id, tag ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat
    FROM summarized_rows
)
SELECT
    id,
    closed_at,
    priority,
    csat,
    closed_at_label,
    priority_ref,
    agent_tier,
    tag,
    ticket_channel_count,
    ticket_channel_count_dense_rank,
    distinct_agent_tier_in_priority,
    ticket_channel_count_sum_1_prior,
    second_csat,
    SUBSTRING(TRIM(agent_tier), 1, 4) AS agent_tier_prefix,
    CONCAT_WS('-', priority, closed_at_label) AS priority_closed_at_label_label
FROM sequenced
ORDER BY csat
