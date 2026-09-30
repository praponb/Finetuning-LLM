WITH filtered AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
    WHERE event_type <> 'created'
),
screened AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND HOUR(t.event_at) BETWEEN 10 AND 13
        )
      AND TO_DATE(s.event_at) < DATE '2025-02-06'
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.event_type_len,
        t.channel AS ticket_channel,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM screened s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
with_activity AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM combined_details s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type IN ('escalated', 'resolved')
        )
      AND s.agent_team <> 'Onboarding'
)
-- Subtotals with ROLLUP
SELECT
    ticket_channel,
    event_type_len,
    SUM(event_type_len) AS total_event_type_len
FROM with_activity
GROUP BY ROLLUP(ticket_channel, event_type_len)
ORDER BY ticket_channel NULLS LAST, event_type_len NULLS LAST
