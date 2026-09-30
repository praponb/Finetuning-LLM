WITH prepared_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('assigned', 'created') THEN 'group_a' WHEN event_type = 'replied' THEN 'group_b' ELSE 'other' END AS event_type_group,
        INITCAP(TRIM(event_type)) AS event_type_clean
    FROM ticket_events
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at NULLS LAST, id) AS prev_event_at,
        NTILE(4) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id) AS event_at_quartile,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_ticket_id_by_event_at)) / 60, 1) AS minutes_since_prev_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at_2,
        DENSE_RANK() OVER w_all_by_event_at_ties AS event_at_dense_rank,
        ROUND(CUME_DIST() OVER (ORDER BY event_at ASC), 3) AS event_at_cume_dist,
        NTILE(4) OVER (PARTITION BY event_type_group ORDER BY event_at DESC, id) AS event_at_quartile_2,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY TO_DATE(event_at), actor ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at_3
    FROM prepared_events
    WINDOW w_ticket_id_by_event_at AS (PARTITION BY ticket_id ORDER BY event_at NULLS LAST, id), w_all_by_event_at_ties AS (ORDER BY event_at)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    actor,
    event_type_group,
    event_type_clean,
    prev_event_at,
    event_at_quartile,
    event_at_dense_rank,
    event_at_quartile_2,
    FLOOR(event_at_quartile_2 / 2) * 2 AS event_at_quartile_2_bucket,
    CONCAT(event_type, '#', CAST(event_at_dense_rank AS STRING)) AS event_type_ref
FROM with_windows
WHERE event_at_quartile BETWEEN 4 AND 7
