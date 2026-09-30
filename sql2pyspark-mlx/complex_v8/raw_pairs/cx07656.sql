WITH in_window AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 7 DAYS)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(hours_since_open / NULLIF(SUM(hours_since_open) OVER w_priority, 0), 4) AS hours_since_open_share_of_priority,
        ROUND(CUME_DIST() OVER w_all_by_hours_since_open_ties, 3) AS hours_since_open_cume_dist
    FROM in_window
    WINDOW w_priority AS (PARTITION BY priority), w_all_by_hours_since_open_ties AS (ORDER BY hours_since_open ASC)
)
SELECT *
FROM (
    SELECT ticket_id, priority, hours_since_open_cume_dist
    FROM windowed
) src
PIVOT (
    AVG(hours_since_open_cume_dist) AS avg_v, SUM(hours_since_open_cume_dist) AS sum_v
    FOR priority IN ('P1', 'P2', 'P4')
)
