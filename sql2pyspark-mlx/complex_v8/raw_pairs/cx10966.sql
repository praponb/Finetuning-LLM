WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DAYOFWEEK(event_at) AS event_at_dayofweek,
        SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix
    FROM ticket_events
    WHERE MONTH(event_at) IN (2, 3)
      AND ticket_id >= 10
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.requester_id AS ticket_requester_id,
        t.csat AS ticket_csat,
        t.priority AS ticket_priority
    FROM base s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label,
        event_at + INTERVAL 15 MINUTES AS event_at_plus_15m
    FROM with_parent
    WHERE ticket_requester_id >= 8
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT event_type, event_at_plus_15m, actor, event_at_dayofweek
    FROM derived
) src
PIVOT (
    SUM(event_at_dayofweek) AS amount, MAX(event_at_dayofweek) AS max_v
    FOR actor IN ('customer', 'system', 'agent')
)
