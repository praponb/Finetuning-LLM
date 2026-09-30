WITH recent AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(TRIM(event_type)) AS event_type_clean,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at,
        TO_DATE(event_at) AS event_at_date
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-02-06 10:00:00'
      AND event_type IN ('reopened', 'replied', 'escalated')
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(days_since_event_at, 0) AS days_since_event_at_filled,
        HOUR(event_at) AS event_at_hour
    FROM recent
    WHERE actor IN ('customer', 'agent')
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        event_type_clean,
        days_since_event_at,
        event_at_date,
        event_at_hour,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type, actor ORDER BY event_at_date ASC NULLS LAST, id), 3) AS event_at_date_percent_rank,
        LEAD(days_since_event_at_filled, 2) OVER (PARTITION BY YEAR(event_at) ORDER BY days_since_event_at, id) AS next2_days_since_event_at_filled
    FROM enriched
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        days_since_event_at,
        event_at_hour,
        event_at_date_percent_rank,
        ROW_NUMBER() OVER w_all_by_next2_days_since_even AS next2_days_since_event_at_filled_seq,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_all_by_next2_days_since_even)) / 60, 1) AS minutes_since_prev_event_at,
        RANK() OVER w_all_by_next2_days_since_even_2 AS next2_days_since_event_at_filled_rank
    FROM windowed
    WINDOW w_all_by_next2_days_since_even AS (ORDER BY next2_days_since_event_at_filled DESC, id), w_all_by_next2_days_since_even_2 AS (ORDER BY next2_days_since_event_at_filled DESC)
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at,
        MAX(event_at) AS last_event_at_2,
        MAX(event_at) AS last_event_at_3
    FROM ticket_events
    GROUP BY ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at,
        t.last_event_at_2,
        t.last_event_at_3
    FROM with_windows s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
-- Final projection
SELECT
    id,
    event_at,
    days_since_event_at,
    event_at_hour,
    event_at_date_percent_rank,
    next2_days_since_event_at_filled_seq,
    minutes_since_prev_event_at,
    next2_days_since_event_at_filled_rank,
    last_event_at_2,
    last_event_at_3,
    ROUND((UNIX_TIMESTAMP(last_event_at_2) - UNIX_TIMESTAMP(event_at)) / 3600, 1) AS hours_event_at_to_last_event_at_2,
    last_event_at + INTERVAL 30 MINUTES AS last_event_at_plus_30m
FROM with_counts
ORDER BY last_event_at_2 NULLS LAST, minutes_since_prev_event_at NULLS LAST
