WITH scoped_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(': ', actor, event_type) AS actor_event_type_label
    FROM ticket_events
    WHERE actor NOT IN ('agent', 'customer')
      AND event_type <> 'assigned'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY MONTH(event_at) ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY MONTH(event_at) ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at,
        ROW_NUMBER() OVER w_event_at_month_by_event_at AS rn,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(event_at) ORDER BY event_at DESC), 3) AS event_at_percent_rank,
        RANK() OVER (PARTITION BY MONTH(event_at) ORDER BY event_at DESC) AS event_at_rank,
        NTILE(3) OVER w_event_at_month_by_event_at AS event_at_tercile
    FROM scoped_events
    WINDOW w_event_at_month_by_event_at AS (PARTITION BY MONTH(event_at) ORDER BY event_at DESC, id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, value
FROM (
    SELECT id, event_at_percent_rank, minutes_since_prev_event_at
    FROM ranked
) src
UNPIVOT (
    value FOR field IN (event_at_percent_rank AS event_at_percent_rank_v, minutes_since_prev_event_at AS m_minutes_since_prev_event_at)
) unp
