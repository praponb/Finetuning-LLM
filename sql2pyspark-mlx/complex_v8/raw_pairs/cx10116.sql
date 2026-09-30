WITH prepared_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        closed_at,
        channel,
        csat,
        DATE_FORMAT(closed_at, 'yyyy-MM-dd') AS closed_at_label,
        ARRAY_JOIN(ARRAY_SORT(tags), ',') AS tags_text,
        ABS(requester_id - 18) AS requester_id_distance
    FROM tickets
),
enriched_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.csat,
        s.closed_at_label,
        s.tags_text,
        s.requester_id_distance,
        su.hired_on AS agent_hired_on
    FROM prepared_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        agent_id,
        closed_at,
        channel,
        tags_text,
        agent_hired_on,
        ABS(csat - 4) AS csat_distance,
        CASE WHEN requester_id_distance >= 15 THEN 'A' WHEN requester_id_distance >= 6 THEN 'B' ELSE 'C' END AS requester_id_distance_tier
    FROM enriched_agent
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.tags_text,
        s.agent_hired_on,
        s.requester_id_distance_tier,
        t.id AS event_id,
        t.event_type,
        t.event_at
    FROM enriched s
    JOIN ticket_events t ON t.ticket_id = s.id
)
-- Subtotals with GROUPING SETS
SELECT
    channel,
    event_id,
    COUNT(DISTINCT channel) AS distinct_channels
FROM line_level_events
GROUP BY GROUPING SETS ((channel, event_id), (channel), (event_id), ())
ORDER BY channel NULLS LAST, event_id NULLS LAST
