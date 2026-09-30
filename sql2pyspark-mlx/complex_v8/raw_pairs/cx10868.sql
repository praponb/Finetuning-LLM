WITH prepared_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket,
        CONCAT_WS('|', channel, priority) AS channel_priority_label,
        UPPER(TRIM(channel)) AS channel_clean
    FROM tickets
    WHERE closed_at >= TIMESTAMP '2025-02-04 23:00:00'
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        requester_id,
        closed_at,
        priority,
        csat,
        requester_id_bucket,
        channel_priority_label,
        channel_clean,
        UPPER(TRIM(channel_priority_label)) AS channel_priority_label_clean
    FROM prepared_tickets
    WHERE priority = 'P4'
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS first_event_at,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS first_event_at_2
    FROM enriched s
),
enriched_2 AS (
    -- Derive helper columns
    SELECT
        *,
        GREATEST(csat, requester_id_bucket) AS greatest_csat_requester_id_bucket,
        GREATEST(requester_id_bucket, requester_id) AS greatest_requester_id_bucket_requester_id,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
    FROM with_counts
)
SELECT
    priority_or_unknown,
    priority,
    csat,
    SUM(CASE WHEN priority = 'P2' THEN 1 ELSE 0 END) AS n_priority_p2,
    SUM(greatest_csat_requester_id_bucket) AS total_greatest_csat_requester_id_bucket
FROM enriched_2
GROUP BY GROUPING SETS ((priority_or_unknown, priority, csat), (priority_or_unknown), ())
