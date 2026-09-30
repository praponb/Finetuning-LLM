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
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        event_type,
        SUM(hours_since_open) FILTER (WHERE event_type IN ('reopened', 'replied')) AS hours_since_open_event_type_in_reopened_repli,
        PERCENTILE_APPROX(hours_since_open, 0.5) AS median_hours_since_open,
        MIN(event_at) AS first_event_at,
        COUNT(*) AS cnt,
        ROUND(STDDEV(hours_since_open), 2) AS stddev_hours_since_open
    FROM in_window
    GROUP BY 1
),
scored AS (
    -- Derive helper columns
    SELECT
        event_type,
        hours_since_open_event_type_in_reopened_repli,
        median_hours_since_open,
        first_event_at,
        cnt,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM stats
),
latest AS (
    -- Keep the latest row per event_type_or_unknown
    SELECT
        event_type,
        hours_since_open_event_type_in_reopened_repli,
        median_hours_since_open,
        first_event_at,
        cnt,
        event_type_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type_or_unknown ORDER BY first_event_at DESC, event_type DESC) AS row_num
        FROM scored
    ) ranked
    WHERE row_num = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT event_type, measure, amount
FROM (
    SELECT event_type, median_hours_since_open, hours_since_open_event_type_in_reopened_repli
    FROM latest
) src
UNPIVOT (
    amount FOR measure IN (median_hours_since_open AS m_median_hours_since_open, hours_since_open_event_type_in_reopened_repli AS m_hours_since_open_event_type_in_reopened_repli)
) unp
