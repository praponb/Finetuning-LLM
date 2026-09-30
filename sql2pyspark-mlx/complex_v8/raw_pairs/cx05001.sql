WITH recent_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        csat,
        INITCAP(TRIM(channel)) AS channel_clean
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'refund')
      AND opened_at >= TIMESTAMP '2025-01-07 13:00:00'
),
grouped AS (
    -- Group-level totals
    SELECT
        agent_id,
        csat,
        SUM(requester_id) AS total_requester_id,
        ROUND(SUM(csat) / NULLIF(SUM(requester_id), 0), 3) AS csat_to_requester_id_ratio,
        COUNT(DISTINCT channel) AS distinct_channels
    FROM recent_tickets
    GROUP BY agent_id, csat
    HAVING MAX(csat) > 1
),
detail_rows AS (
    -- One row per ticket
    SELECT
        s.agent_id,
        s.csat,
        s.csat_to_requester_id_ratio,
        s.distinct_channels,
        t.id AS ticket_id,
        t.channel AS ticket_channel,
        t.requester_id AS ticket_requester_id
    FROM grouped s
    LEFT JOIN tickets t ON t.agent_id = s.agent_id AND t.channel <> 'phone'
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count,
        (SELECT COUNT(*) FILTER (WHERE t.channel NOT IN ('phone', 'email')) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_channel_count,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count_2
    FROM detail_rows s
)
-- Final projection
SELECT
    agent_id,
    csat,
    csat_to_requester_id_ratio,
    distinct_channels,
    ticket_id,
    ticket_channel,
    ticket_requester_id,
    ticket_channel_count,
    ticket_count_2,
    UPPER(TRIM(ticket_channel)) AS ticket_channel_clean
FROM summarized_rows
