WITH cleaned_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        CONCAT_WS('-', actor, event_type) AS actor_event_type_label
    FROM ticket_events
    WHERE event_type IS NOT NULL
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY TO_DATE(event_at_month) ORDER BY event_at, id) AS event_at_seq,
        FIRST_VALUE(event_at_month) IGNORE NULLS OVER (PARTITION BY ticket_id ORDER BY event_at_month DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_month,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(event_at_month) ORDER BY event_at), 3) AS event_at_cume_dist,
        ROUND(PERCENT_RANK() OVER w_event_at_month_to_date_ticke, 3) AS event_at_month_percent_rank,
        FIRST_VALUE(event_at) OVER (PARTITION BY TO_DATE(event_at_month), ticket_id ORDER BY event_at_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        NTH_VALUE(event_at_month, 2) OVER (PARTITION BY ticket_id ORDER BY event_at_month DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_month,
        LAST_VALUE(event_at_month) IGNORE NULLS OVER (PARTITION BY event_type, actor ORDER BY event_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_month,
        ROUND((UNIX_TIMESTAMP(event_at_month) - UNIX_TIMESTAMP(LAG(event_at_month) OVER w_event_at_month_to_date_ticke_3)) / 60, 1) AS minutes_since_prev_event_at_month
    FROM cleaned_events
    WINDOW w_event_at_month_to_date_ticke AS (PARTITION BY TO_DATE(event_at_month), ticket_id ORDER BY event_at_month), w_event_at_month_to_date_ticke_3 AS (PARTITION BY TO_DATE(event_at_month), ticket_id ORDER BY event_at_month, id)
),
first_rows AS (
    -- First row per group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_month,
        actor_event_type_label,
        first_known_event_at_month,
        event_at_cume_dist,
        event_at_month_percent_rank,
        first_event_at,
        second_event_at_month,
        last_known_event_at_month,
        minutes_since_prev_event_at_month
    FROM sequenced
    WHERE event_at_seq = 1
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    event_at_month,
    actor_event_type_label,
    first_known_event_at_month,
    event_at_month_percent_rank,
    first_event_at,
    minutes_since_prev_event_at_month,
    ROUND((UNIX_TIMESTAMP(event_at_month) - UNIX_TIMESTAMP(second_event_at_month)) / 3600, 1) AS hours_second_event_at_month_to_event_at_month,
    CASE WHEN event_type IN ('created', 'escalated') THEN 'group_a' WHEN event_type = 'resolved' THEN 'group_b' ELSE 'other' END AS event_type_group
FROM first_rows
ORDER BY event_at_month DESC NULLS FIRST, id
LIMIT 20
