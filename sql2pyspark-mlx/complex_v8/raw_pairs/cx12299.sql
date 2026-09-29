WITH cleaned_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        csat,
        requester_id + csat AS requester_id_plus_csat,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 60, 1) AS minutes_opened_at_to_closed_at,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown
    FROM tickets
    WHERE agent_id >= 2
),
joined_agent AS (
    -- Attach agent attributes
    SELECT s.*, su.tier AS agent_tier
    FROM cleaned_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        opened_at,
        csat,
        requester_id_plus_csat,
        agent_tier,
        COALESCE(requester_id, 0) AS requester_id_filled,
        ABS(requester_id_plus_csat - 10) AS requester_id_plus_csat_distance
    FROM joined_agent
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MIN(event_at) AS first_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at
    FROM annotated s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
)
SELECT
    id,
    opened_at,
    csat,
    agent_tier,
    requester_id_filled,
    requester_id_plus_csat_distance,
    first_event_at,
    SUBSTRING(TRIM(agent_tier), 1, 3) AS agent_tier_prefix,
    TO_DATE(first_event_at) AS first_event_at_date
FROM summarized_rows
ORDER BY id, first_event_at
