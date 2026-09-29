WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('WEEK', event_at) AS event_at_week
    FROM ticket_events
    WHERE event_type IS NOT NULL
      AND HOUR(event_at) BETWEEN 10 AND 16
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        event_at_week,
        ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id) AS seq_no,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY actor, TO_DATE(event_at) ORDER BY event_at_week, id))) / 60, 1) AS minutes_since_prev_event_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        LEAD(event_at) OVER (PARTITION BY actor, TO_DATE(event_at) ORDER BY event_at_week, id) AS next_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at_3,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id ORDER BY event_at, id), 3) AS event_at_cume_dist,
        ROUND((UNIX_TIMESTAMP(event_at_week) - UNIX_TIMESTAMP(LAG(event_at_week) OVER w_ticket_id_by_event_at_2)) / 60, 1) AS minutes_since_prev_event_at_week,
        ROW_NUMBER() OVER w_ticket_id_by_event_at_2 AS seq_no_2,
        FIRST_VALUE(event_at_week) IGNORE NULLS OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_week,
        FIRST_VALUE(event_at_week) OVER (PARTITION BY actor, TO_DATE(event_at) ORDER BY event_at_week, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at_week,
        RANK() OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id) AS event_at_rank
    FROM scoped
    WINDOW w_ticket_id_by_event_at_2 AS (PARTITION BY ticket_id ORDER BY event_at, id)
),
leaders AS (
    -- Top 2 per group
    SELECT *
    FROM ranked
    WHERE event_at_rank <= 2
      AND MONTH(event_at) IN (1, 3)
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    event_at_week,
    seq_no,
    minutes_since_prev_event_at,
    event_at_cume_dist,
    minutes_since_prev_event_at_week,
    first_event_at_week,
    event_at_rank,
    CONCAT(event_type, '#', CAST(event_at_rank AS STRING)) AS event_type_ref,
    LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
FROM leaders
ORDER BY id, event_at_rank
