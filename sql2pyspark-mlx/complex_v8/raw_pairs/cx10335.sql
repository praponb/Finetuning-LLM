WITH cleaned AS (
    -- Filter tickets
    SELECT *
    FROM tickets
    WHERE requester_id >= 8
      AND agent_id IS NOT NULL
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        requester_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        COALESCE(csat, 0) AS csat_filled,
        UPPER(TRIM(priority)) AS priority_clean
    FROM cleaned
    WHERE priority = 'P1'
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_count,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_count_2
    FROM tagged s
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT csat, channel, requester_id
    FROM with_counts
) src
PIVOT (
    COUNT(requester_id)
    FOR channel IN ('email', 'web')
)
ORDER BY csat
