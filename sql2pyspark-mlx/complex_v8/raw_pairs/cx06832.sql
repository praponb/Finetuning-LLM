WITH prepared_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('WEEK', event_at) AS event_at_week,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM ticket_events
    WHERE ticket_id >= 19
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_at_week,
        s.event_type_slug,
        t.tags AS ticket_tags
    FROM prepared_events s
    JOIN tickets t ON t.id = s.ticket_id AND t.channel IS NOT NULL
    WHERE ARRAY_CONTAINS(t.tags, 'upgrade')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(event_at_week) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', event_at), event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_week,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id ORDER BY event_at NULLS LAST), 3) AS event_at_cume_dist,
        DENSE_RANK() OVER (ORDER BY event_at, id) AS event_at_dense_rank,
        ROUND(PERCENT_RANK() OVER w_event_at_month_event_type_by_2, 3) AS event_at_percent_rank,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_ticket_id_by_event_at)) / 60, 1) AS minutes_since_prev_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor ORDER BY event_at_week ASC, id), 3) AS event_at_week_percent_rank,
        NTILE(10) OVER (PARTITION BY actor ORDER BY event_at_week ASC, id) AS event_at_week_decile,
        ROUND(PERCENT_RANK() OVER w_ticket_id_by_event_at, 3) AS event_at_percent_rank_2,
        DENSE_RANK() OVER w_all_by_event_at_ties AS event_at_dense_rank_2,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', event_at), event_type ORDER BY event_at, id) AS seq_no
    FROM combined_details
    WINDOW w_event_at_month_event_type_by_2 AS (PARTITION BY DATE_TRUNC('MONTH', event_at), event_type ORDER BY event_at, id), w_ticket_id_by_event_at AS (PARTITION BY ticket_id ORDER BY event_at NULLS LAST, id), w_all_by_event_at_ties AS (ORDER BY event_at)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    event_at_week,
    event_type_slug,
    ticket_tags,
    event_at_cume_dist,
    event_at_dense_rank,
    minutes_since_prev_event_at,
    event_at_week_percent_rank,
    event_at_week_decile,
    event_at_percent_rank_2,
    first_known_event_at,
    seq_no,
    CASE WHEN event_type IN ('resolved', 'created') THEN 'group_a' WHEN event_type = 'replied' THEN 'group_b' ELSE 'other' END AS event_type_group
FROM with_windows
ORDER BY first_known_event_at DESC, minutes_since_prev_event_at
