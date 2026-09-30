WITH base_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel
    FROM tickets
    WHERE csat >= 2
      AND HOUR(closed_at) BETWEEN 6 AND 10
),
joined_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        su.hired_on AS agent_hired_on
    FROM base_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
combined_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on_2
    FROM joined_agent s
    JOIN support_agents su ON su.id = s.agent_id
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', closed_at) AS days_since_closed_at,
        DATE_FORMAT(closed_at, 'MMM yyyy') AS closed_at_label,
        SUBSTRING(TRIM(channel), 1, 4) AS channel_prefix
    FROM combined_details_agent
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        closed_at + INTERVAL 30 MINUTES AS closed_at_plus_30m,
        YEAR(closed_at) AS closed_at_year,
        CASE WHEN days_since_closed_at >= 595 THEN 'HIGH' WHEN days_since_closed_at >= 315 THEN 'MEDIUM' ELSE 'LOW' END AS days_since_closed_at_tier
    FROM tagged
    WHERE priority = 'P3'
),
with_children AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.days_since_closed_at,
        s.channel_prefix,
        s.closed_at_plus_30m,
        s.closed_at_year,
        s.days_since_closed_at_tier,
        t.id AS event_id,
        t.event_at,
        t.actor AS event_actor,
        t.event_type
    FROM enriched s
    JOIN ticket_events t ON t.ticket_id = s.id AND t.event_type IN ('reopened', 'resolved', 'replied')
)
-- Final projection
SELECT
    id,
    closed_at,
    priority,
    days_since_closed_at,
    channel_prefix,
    closed_at_plus_30m,
    closed_at_year,
    event_at,
    event_type,
    ABS(closed_at_year - 2025) AS closed_at_year_distance
FROM with_children
ORDER BY priority
