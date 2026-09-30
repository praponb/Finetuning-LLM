WITH scoped_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        ROUND(requester_id / NULLIF(csat, 0), 3) AS requester_id_per_csat
    FROM tickets
    WHERE opened_at >= TIMESTAMP '2025-01-25 08:00:00'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at), agent_id ORDER BY opened_at DESC, id) AS opened_at_seq,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at), agent_id ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        COUNT(*) OVER (PARTITION BY MONTH(closed_at), csat) AS rows_in_closed_at_month_csat,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY MONTH(closed_at), csat), 0), 4) AS requester_id_share_of_closed_at_month_csat
    FROM scoped_tickets
),
exploded_detail_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.csat,
        s.requester_id_per_csat,
        s.opened_at_seq,
        s.running_count,
        s.rows_in_closed_at_month_csat,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at,
        t.event_type
    FROM sequenced s
    JOIN ticket_events t ON t.ticket_id = s.id
)
SELECT id, event_id, metric, reading
FROM (
    SELECT id, event_id, rows_in_closed_at_month_csat, csat, opened_at_seq, running_count
    FROM exploded_detail_events
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric IN (rows_in_closed_at_month_csat, csat, opened_at_seq, running_count)
) unp
ORDER BY id, event_id, metric
