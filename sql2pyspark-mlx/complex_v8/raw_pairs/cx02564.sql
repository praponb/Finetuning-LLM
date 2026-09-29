WITH recent_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        tags,
        SUBSTRING(TRIM(priority), 1, 3) AS priority_prefix,
        LENGTH(TRIM(channel)) AS channel_len
    FROM tickets
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM recent_tickets
    WHERE channel_len > (SELECT AVG(channel_len) FROM recent_tickets)
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_count
    FROM filtered s
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT channel_len, channel, event_count
    FROM summarized_rows
) src
PIVOT (
    MIN(event_count)
    FOR channel IN ('email', 'web', 'phone', 'chat')
)
ORDER BY channel_len
