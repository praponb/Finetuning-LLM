WITH recent AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        csat,
        CASE WHEN requester_id >= 19 THEN 'A' WHEN requester_id >= 7 THEN 'B' ELSE 'C' END AS requester_id_tier,
        LOWER(TRIM(channel)) AS channel_clean,
        SUBSTRING(TRIM(priority), 1, 2) AS priority_prefix
    FROM tickets
    WHERE SIZE(tags) >= 2
),
without_matches AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM recent s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.actor = 'agent'
              AND t.event_type IN ('reopened', 'replied')
        )
      AND UPPER(TRIM(s.channel_clean)) LIKE 'W%'
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at_2,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at_3
    FROM without_matches s
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT last_event_at_3, channel, csat
    FROM summarized_rows
) src
PIVOT (
    MAX(csat)
    FOR channel IN ('phone', 'chat', 'web', 'email')
)
