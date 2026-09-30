WITH filtered_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug
    FROM tickets
    WHERE agent_id IS NOT NULL
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        COUNT(*) AS ticket_count,
        MIN(closed_at) AS first_ticket_closed_at,
        COUNT(*) AS ticket_count_2
    FROM tickets
    GROUP BY agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count,
        t.first_ticket_closed_at,
        COALESCE(t.ticket_count_2, 0) AS ticket_count_2
    FROM filtered_tickets s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT SUM(t.requester_id) FROM tickets t WHERE t.agent_id = s.agent_id) AS total_ticket_requester_id,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count_3
    FROM with_stats s
),
agent_id_stats AS (
    -- Max ticket_count_3 per agent_id
    SELECT agent_id, MAX(ticket_count_3) AS max_ticket_count_3
    FROM summarized_rows
    GROUP BY agent_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_ticket_count_3,
        ROUND(s.ticket_count_3 - g.max_ticket_count_3, 2) AS ticket_count_3_minus_max
    FROM summarized_rows s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.ticket_count_3 = g.max_ticket_count_3
),
unique_rows AS (
    -- Keep the latest row per agent_id
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        channel_slug,
        ticket_count,
        first_ticket_closed_at,
        ticket_count_2,
        total_ticket_requester_id,
        ticket_count_3,
        max_ticket_count_3,
        ticket_count_3_minus_max
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY ticket_count_3_minus_max DESC, id DESC) AS recency_rank
        FROM compared
    ) ranked
    WHERE recency_rank = 1
),
with_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.channel_slug,
        s.ticket_count,
        s.first_ticket_closed_at,
        s.ticket_count_2,
        s.ticket_count_3,
        s.max_ticket_count_3,
        s.ticket_count_3_minus_max,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM unique_rows s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE UPPER(TRIM(su.agent_name)) LIKE 'H%'
)
-- Final projection
SELECT
    id,
    agent_id,
    closed_at,
    priority,
    channel,
    csat,
    channel_slug,
    ticket_count,
    first_ticket_closed_at,
    ticket_count_2,
    max_ticket_count_3,
    ticket_count_3_minus_max,
    LENGTH(TRIM(channel_slug)) AS channel_slug_len,
    DATE_FORMAT(closed_at, 'yyyy-MM-dd HH:00') AS closed_at_label
FROM with_details_agent
WHERE ticket_count_3 > 3
ORDER BY ticket_count_3_minus_max, id
LIMIT 10
