WITH base_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        closed_at,
        channel,
        csat,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown,
        CONCAT(priority, '#', CAST(requester_id AS STRING)) AS priority_ref
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'outage')
),
stats AS (
    -- Group-level totals
    SELECT
        channel,
        agent_id,
        priority_or_unknown,
        ROUND(STDDEV(csat), 2) AS stddev_csat,
        MIN(csat) AS min_csat,
        COUNT(*) AS n_rows,
        MAX(closed_at) AS last_closed_at
    FROM base_tickets
    GROUP BY channel, agent_id, priority_or_unknown
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        COUNT(*) AS ticket_count,
        COUNT(*) AS ticket_count_2,
        COUNT(CASE WHEN priority = 'P2' THEN 1 END) AS ticket_priority_count
    FROM tickets
    GROUP BY agent_id
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count,
        COALESCE(t.ticket_count_2, 0) AS ticket_count_2,
        COALESCE(t.ticket_priority_count, 0) AS ticket_priority_count
    FROM stats s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
)
SELECT *
FROM (
    SELECT last_closed_at, priority_or_unknown, ticket_count
    FROM summarized_rows
) src
PIVOT (
    COUNT(ticket_count) AS count_v, MAX(ticket_count) AS max_v
    FOR priority_or_unknown IN ('P1', 'P3', 'P2', 'UNKNOWN')
)
