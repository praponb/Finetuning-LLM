WITH base AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat
    FROM tickets
    WHERE channel = 'phone'
      AND agent_id IS NOT NULL
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        requester_id,
        priority,
        channel,
        ROUND(requester_id / NULLIF(csat, 0), 3) AS requester_id_per_csat
    FROM base
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_count
    FROM annotated s
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(requester_id_per_csat - 5.0) AS requester_id_per_csat_distance,
        CONCAT_WS('|', channel, priority) AS channel_priority_label
    FROM with_counts
    WHERE requester_id_per_csat > 4.0
)
SELECT
    id,
    priority,
    channel,
    requester_id_per_csat,
    event_count,
    requester_id_per_csat_distance,
    channel_priority_label
FROM scored
ORDER BY id DESC
