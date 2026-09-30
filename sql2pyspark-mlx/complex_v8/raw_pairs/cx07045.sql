WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_event_at_month_event_type_by)) / 60, 1) AS minutes_since_prev_event_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY MONTH(event_at), event_type ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(event_at), event_type ORDER BY event_at ASC, id), 3) AS event_at_cume_dist,
        LEAD(event_at, 2) OVER (PARTITION BY MONTH(event_at), event_type ORDER BY event_at ASC, id) AS next2_event_at,
        ROUND(PERCENT_RANK() OVER w_event_at_month_event_type_by_3, 3) AS event_at_percent_rank
    FROM base
    WINDOW w_event_at_month_event_type_by AS (PARTITION BY MONTH(event_at), event_type ORDER BY event_at ASC, id), w_event_at_month_event_type_by_3 AS (PARTITION BY MONTH(event_at), event_type ORDER BY event_at ASC)
),
filtered AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor IS NOT NULL
              AND TO_DATE(t.event_at) < DATE '2025-02-24'
        )
),
exploded_detail_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.minutes_since_prev_event_at,
        s.second_event_at,
        s.event_at_percent_rank,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.actor AS event_actor,
        t.event_at AS event_event_at
    FROM filtered s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
unique_rows AS (
    -- Keep the latest row per event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        minutes_since_prev_event_at,
        second_event_at,
        event_at_percent_rank,
        event_id,
        event_event_type,
        event_actor,
        event_event_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_at DESC, id DESC, event_id DESC) AS rn
        FROM exploded_detail_events
    ) ranked
    WHERE rn = 1
)
SELECT *
FROM (
    SELECT event_event_at, actor, minutes_since_prev_event_at
    FROM unique_rows
) src
PIVOT (
    MAX(minutes_since_prev_event_at)
    FOR actor IN ('system', 'customer', 'agent')
)
ORDER BY event_event_at
