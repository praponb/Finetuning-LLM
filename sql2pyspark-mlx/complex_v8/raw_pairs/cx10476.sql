WITH filtered_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label
    FROM ticket_events
    WHERE ticket_id <= 53
      AND actor = 'system'
),
derived AS (
    -- Add calculated columns
    SELECT *, UPPER(TRIM(actor)) AS actor_clean
    FROM filtered_events
),
qualified AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM derived s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor = 'system'
              AND t.event_at < TIMESTAMP '2025-03-02 14:00:00'
        )
      AND HOUR(s.event_at) BETWEEN 6 AND 11
),
with_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.event_at_label,
        s.actor_clean,
        t.channel AS ticket_channel,
        t.closed_at AS ticket_closed_at,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team
    FROM qualified s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MIN(event_at) AS first_event_at,
        MIN(event_at) AS first_event_at_2
    FROM ticket_events
    GROUP BY ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at,
        t.first_event_at_2
    FROM with_details_ticket s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
derived_2 AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        actor_clean,
        ticket_channel,
        agent_hired_on,
        first_event_at_2,
        TO_DATE(event_at) AS event_at_date
    FROM with_counts
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(actor_clean), 1, 3) AS actor_clean_prefix,
        CASE WHEN ticket_channel IN ('chat', 'web') THEN 'group_a' WHEN ticket_channel = 'phone' THEN 'group_b' ELSE 'other' END AS ticket_channel_group
    FROM derived_2
)
-- Final projection
SELECT
    id,
    ticket_id,
    actor,
    actor_clean,
    ticket_channel,
    first_event_at_2,
    event_at_date,
    actor_clean_prefix,
    ticket_channel_group,
    INITCAP(TRIM(actor_clean)) AS actor_clean_clean,
    DATEDIFF(agent_hired_on, event_at_date) AS days_event_at_date_to_agent_hired_on
FROM annotated
WHERE agent_hired_on >= DATE '2024-05-02'
ORDER BY event_at_date, actor_clean_clean DESC
